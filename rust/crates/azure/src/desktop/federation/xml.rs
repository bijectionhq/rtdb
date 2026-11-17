use std::collections::{BTreeMap, BTreeSet};
use quick_xml::{events::Event, Reader};
use super::super::{invalid, MAX_RESPONSE_BYTES};

type Result<T> = azure_core::Result<T>;
const XML: &str = "http://www.w3.org/XML/1998/namespace";
const XMLNS: &str = "http://www.w3.org/2000/xmlns/";
const MAX_NODES: usize = 65_536;
const MAX_DEPTH: usize = 256;

pub(super) struct Document { nodes: Vec<Node>, xml11: bool, retained_bytes: usize }
struct Node { parent: Option<usize>, children: Vec<usize>, value: Value }
enum Value {
    Element { name: String, namespace: String, attributes: BTreeMap<String, Attribute> },
    Text(String), Cdata(String), Comment(String), Instruction(String),
}
struct Attribute { namespace: String, value: String }

impl Document {
    pub fn parse(text: &str) -> Result<Self> {
        if text.len() > MAX_RESPONSE_BYTES { return Err(invalid("Azure federation XML exceeds its byte limit")); }
        let mut reader = Reader::from_str(text);
        reader.config_mut().check_comments = true;
        let mut document = Self { nodes: Vec::new(), xml11: false, retained_bytes: 0 };
        let mut stack = Vec::<usize>::new();
        let mut namespaces = vec![(0, "xml".to_owned(), XML.to_owned())];
        let mut root_seen = false;
        let mut declaration_allowed = true;
        loop {
            let event = reader.read_event().map_err(|_| invalid("Azure federation XML is invalid"))?;
            match event {
                Event::Start(ref start) | Event::Empty(ref start) => {
                    if stack.len() >= MAX_DEPTH { return Err(invalid("Azure federation XML exceeds its nesting limit")); }
                    if stack.is_empty() {
                        if root_seen { return Err(invalid("Azure federation XML has multiple roots")); }
                        root_seen = true;
                    }
                    let depth = stack.len() + 1;
                    let name = utf8(start.name().as_ref())?.to_owned();
                    if !valid_name(&name) || name.starts_with("xmlns:") { return Err(invalid("Azure federation XML element name is invalid")); }
                    let mut attributes = BTreeMap::<String, Attribute>::new();
                    for attribute in start.attributes() {
                        let attribute = attribute.map_err(|_| invalid("Azure federation XML attribute is invalid"))?;
                        let name = utf8(attribute.key.as_ref())?.to_owned();
                        if !valid_name(&name) { return Err(invalid("Azure federation XML attribute name is invalid")); }
                        let raw = normalize_attribute(utf8(&attribute.value)?, document.xml11)?;
                        let value = quick_xml::escape::unescape(&raw).map_err(|_| invalid("Azure federation XML attribute is invalid"))?.into_owned();
                        if !valid_chars(&value, document.xml11) { return Err(invalid("Azure federation XML contains an invalid character")); }
                        if name == "xmlns" || name.starts_with("xmlns:") {
                            let prefix = name.strip_prefix("xmlns:").unwrap_or("");
                            if prefix == "xmlns" || (prefix == "xml" && value != XML) || value == XMLNS || (prefix != "xml" && value == XML) || (!prefix.is_empty() && value.is_empty()) {
                                return Err(invalid("Azure federation XML namespace is invalid"));
                            }
                            namespaces.push((depth, prefix.to_owned(), value.clone()));
                        }
                        attributes.insert(name, Attribute { namespace: String::new(), value });
                    }
                    let resolve = |name: &str, attribute: bool| -> Result<String> {
                        let prefix = name.split_once(':').map(|(prefix, _)| prefix);
                        if prefix == Some("xmlns") || attribute && name == "xmlns" { return Ok(XMLNS.into()); }
                        if attribute && prefix.is_none() { return Ok(String::new()); }
                        let prefix = prefix.unwrap_or("");
                        namespaces.iter().rev().find(|(_, candidate, _)| candidate == prefix).map(|(_, _, uri)| uri.clone())
                            .or_else(|| prefix.is_empty().then(String::new)).ok_or_else(|| invalid("Azure federation XML namespace is unbound"))
                    };
                    let namespace = resolve(&name, false)?;
                    let mut expanded = BTreeSet::new();
                    for (name, attribute) in &mut attributes {
                        attribute.namespace = resolve(name, true)?;
                        if !expanded.insert((attribute.namespace.clone(), local(name).to_owned())) { return Err(invalid("Azure federation XML contains duplicate attributes")); }
                    }
                    let id = document.append(stack.last().copied(), Value::Element { name, namespace, attributes })?;
                    if matches!(event, Event::Start(_)) { stack.push(id); }
                    else { namespaces.retain(|(scope, _, _)| *scope < depth); }
                }
                Event::End(_) => {
                    let depth = stack.len();
                    if stack.pop().is_none() { return Err(invalid("Azure federation XML end tag is invalid")); }
                    namespaces.retain(|(scope, _, _)| *scope < depth);
                }
                Event::Text(text) => {
                    let text = (if document.xml11 { text.xml11_content() } else { text.xml10_content() }).map_err(|_| invalid("Azure federation XML text is invalid"))?;
                    if !valid_literal(&text, document.xml11) { return Err(invalid("Azure federation XML contains an invalid character")); }
                    document.text(stack.last().copied(), text.into_owned())?;
                }
                Event::GeneralRef(reference) => {
                    let name = reference.decode().map_err(|_| invalid("Azure federation XML entity is invalid"))?;
                    let encoded = format!("&{name};");
                    let value = quick_xml::escape::unescape(&encoded).map_err(|_| invalid("Azure federation XML entity is invalid"))?;
                    document.text(stack.last().copied(), value.into_owned())?;
                }
                Event::CData(text) => {
                    let parent = stack.last().copied().ok_or_else(|| invalid("Azure federation XML CDATA is outside its root"))?;
                    let text = (if document.xml11 { text.xml11_content() } else { text.xml10_content() }).map_err(|_| invalid("Azure federation XML CDATA is invalid"))?.into_owned();
                    if !valid_literal(&text, document.xml11) { return Err(invalid("Azure federation XML contains an invalid character")); }
                    document.append(Some(parent), Value::Cdata(text))?;
                }
                Event::Comment(text) => {
                    let text = (if document.xml11 { text.xml11_content() } else { text.xml10_content() }).map_err(|_| invalid("Azure federation XML comment is invalid"))?.into_owned();
                    if !valid_literal(&text, document.xml11) { return Err(invalid("Azure federation XML comment is invalid")); }
                    document.append(stack.last().copied(), Value::Comment(text))?;
                }
                Event::PI(text) => { document.append(stack.last().copied(), Value::Instruction(utf8(text.as_ref())?.to_owned()))?; }
                Event::Decl(declaration) => {
                    if !declaration_allowed || root_seen || !stack.is_empty() { return Err(invalid("Azure federation XML declaration is misplaced")); }
                    document.xml11 = match declaration.version().map_err(|_| invalid("Azure federation XML version is invalid"))?.as_ref() {
                        b"1.0" => false, b"1.1" => true, _ => return Err(invalid("Azure federation XML version is invalid")),
                    };
                    if declaration.encoding().transpose().map_err(|_| invalid("Azure federation XML encoding is invalid"))?.is_some_and(|encoding| !encoding.eq_ignore_ascii_case(b"utf-8")) {
                        return Err(invalid("Azure federation XML requires UTF-8"));
                    }
                }
                Event::DocType(_) => return Err(invalid("Azure federation XML does not permit a document type")),
                Event::Eof => break,
            }
            declaration_allowed = false;
        }
        if !root_seen || !stack.is_empty() { return Err(invalid("Azure federation XML is incomplete")); }
        Ok(document)
    }

    fn append(&mut self, parent: Option<usize>, value: Value) -> Result<usize> {
        if self.nodes.len() == MAX_NODES { return Err(invalid("Azure federation XML exceeds its node limit")); }
        let bytes = 512 + match &value {
            Value::Element { name, namespace, attributes } => name.len() + namespace.len() + attributes.iter().map(|(name, attribute)| name.len() + attribute.namespace.len() + attribute.value.len() + 128).sum::<usize>(),
            Value::Text(value) | Value::Cdata(value) | Value::Comment(value) | Value::Instruction(value) => value.len(),
        };
        self.retain(bytes)?;
        let id = self.nodes.len();
        self.nodes.push(Node { parent, children: Vec::new(), value });
        if let Some(parent) = parent { self.nodes[parent].children.push(id); }
        Ok(id)
    }

    fn retain(&mut self, bytes: usize) -> Result<()> {
        self.retained_bytes = self.retained_bytes.checked_add(bytes).filter(|bytes| *bytes <= 8 * MAX_RESPONSE_BYTES)
            .ok_or_else(|| invalid("Azure federation XML exceeds its retained byte limit"))?;
        Ok(())
    }

    fn text(&mut self, parent: Option<usize>, text: String) -> Result<()> {
        if !valid_chars(&text, self.xml11) { return Err(invalid("Azure federation XML contains an invalid character")); }
        let Some(parent) = parent else {
            return if text.chars().all(|character| matches!(character, ' ' | '\r' | '\n' | '\t')) { Ok(()) } else { Err(invalid("Azure federation XML text is outside its root")) };
        };
        if let Some(previous) = self.nodes[parent].children.last().copied() {
            if matches!(&self.nodes[previous].value, Value::Text(_)) {
                self.retain(text.len())?;
                if let Value::Text(previous) = &mut self.nodes[previous].value { previous.push_str(&text); }
                return Ok(());
            }
        }
        self.append(Some(parent), Value::Text(text))?;
        Ok(())
    }

    pub fn named(&self, id: usize, namespace: &str, name: &str) -> bool {
        matches!(&self.nodes[id].value, Value::Element { name: actual, namespace: uri, .. } if uri == namespace && local(actual) == name)
    }
    pub fn descendants(&self, namespace: &str, name: &str) -> Vec<usize> { (0..self.nodes.len()).filter(|id| self.named(*id, namespace, name)).collect() }
    pub fn children(&self, id: usize, namespace: &str, name: &str) -> Vec<usize> { self.nodes[id].children.iter().copied().filter(|id| self.named(*id, namespace, name)).collect() }
    pub fn path(&self, roots: Vec<usize>, steps: &[(&str, &str)]) -> Vec<usize> {
        steps.iter().fold(roots, |parents, (namespace, name)| parents.into_iter().flat_map(|id| self.children(id, namespace, name)).collect())
    }
    pub fn attribute(&self, id: usize, name: &str) -> Option<&str> {
        match &self.nodes[id].value { Value::Element { attributes, .. } => attributes.get(name).map(|attribute| attribute.value.as_str()), _ => None }
    }
    pub fn parent(&self, id: usize) -> Option<usize> { self.nodes[id].parent }
    pub fn text_content(&self, id: usize) -> String {
        let mut text = String::new();
        self.append_text(id, &mut text);
        text
    }
    fn append_text(&self, id: usize, text: &mut String) {
        match &self.nodes[id].value {
            Value::Text(value) | Value::Cdata(value) => text.push_str(value),
            Value::Element { .. } => for child in &self.nodes[id].children { self.append_text(*child, text); },
            _ => (),
        }
    }
    pub fn first_child_text(&self, id: usize) -> Option<String> { self.nodes[id].children.first().map(|child| self.text_content(*child)) }

    pub fn inner_xml(&self, id: usize) -> Result<String> {
        let mut result = String::new();
        let mut writer = String::new();
        for child in &self.nodes[id].children {
            self.serialize(*child, &mut writer, &BTreeMap::from([("xml".to_owned(), XML.to_owned()), (String::new(), String::new())]))?;
            if result.len().checked_add(writer.len()).is_none_or(|bytes| bytes > MAX_RESPONSE_BYTES) { return Err(invalid("Azure federation assertion exceeds its byte limit")); }
            result.push_str(&writer);
        }
        Ok(result.trim_matches(|character| character <= '\u{20}').to_owned())
    }

    fn serialize(&self, id: usize, output: &mut String, inherited: &BTreeMap<String, String>) -> Result<()> {
        match &self.nodes[id].value {
            Value::Text(text) => output.push_str(&escape(text, false)),
            Value::Cdata(text) => { output.push_str("<![CDATA["); output.push_str(text); output.push_str("]]>"); }
            Value::Comment(text) => { output.push_str("<!--"); output.push_str(text); output.push_str("-->"); }
            Value::Instruction(text) => { output.push_str("<?"); output.push_str(text); output.push_str("?>"); }
            Value::Element { name, namespace, attributes } => {
                output.push('<'); output.push_str(name);
                let mut ordered = attributes.iter().collect::<Vec<_>>();
                ordered.sort_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));
                let mut namespaces = inherited.clone();
                let mut serialized = Vec::<(String, String)>::new();
                for (name, attribute) in ordered.iter().copied() {
                    if name == "xmlns" || name.starts_with("xmlns:") {
                        let prefix = name.rsplit_once(':').map_or("", |(_, prefix)| prefix);
                        declare(prefix, &attribute.value, &mut namespaces, &mut serialized);
                    }
                }
                for (name, attribute) in ordered.iter().copied() {
                    if name == "xmlns" || name.starts_with("xmlns:") { continue; }
                    if !attribute.namespace.is_empty() {
                        let prefix = name.split_once(':').map_or("ns0", |(prefix, _)| prefix);
                        declare(prefix, &attribute.namespace, &mut namespaces, &mut serialized);
                    }
                    serialized.push((name.clone(), attribute.value.clone()));
                }
                let prefix = name.split_once(':').map_or("", |(prefix, _)| prefix);
                declare(prefix, namespace, &mut namespaces, &mut serialized);
                for (name, value) in serialized {
                    output.push(' '); output.push_str(&name); output.push_str("=\""); output.push_str(&escape(&value, true)); output.push('"');
                }
                if self.nodes[id].children.is_empty() { output.push_str("/>"); }
                else {
                    output.push('>');
                    for child in &self.nodes[id].children { self.serialize(*child, output, &namespaces)?; }
                    output.push_str("</"); output.push_str(name); output.push('>');
                }
            }
        }
        if output.len() > MAX_RESPONSE_BYTES { return Err(invalid("Azure federation assertion exceeds its byte limit")); }
        Ok(())
    }
}

fn declare(prefix: &str, namespace: &str, namespaces: &mut BTreeMap<String, String>, attributes: &mut Vec<(String, String)>) {
    if namespaces.get(prefix).map(String::as_str) == Some(namespace) { return; }
    namespaces.insert(prefix.to_owned(), namespace.to_owned());
    if !prefix.is_empty() && namespace.is_empty() { return; }
    let name = if prefix.is_empty() { "xmlns".to_owned() } else { format!("xmlns:{prefix}") };
    if let Some((_, value)) = attributes.iter_mut().find(|(key, _)| key == &name) { *value = namespace.to_owned(); }
    else { attributes.push((name, namespace.to_owned())); }
}

fn utf8(bytes: &[u8]) -> Result<&str> { std::str::from_utf8(bytes).map_err(|_| invalid("Azure federation XML is not UTF-8")) }
fn local(name: &str) -> &str { name.rsplit(':').next().unwrap_or(name) }
fn valid_name(name: &str) -> bool {
    let mut parts = name.split(':');
    let valid = |part: &str| {
        let mut characters = part.chars();
        characters.next().is_some_and(name_start) && characters.all(|character| name_start(character) || matches!(character, '-' | '.' | '0'..='9' | '\u{b7}' | '\u{300}'..='\u{36f}' | '\u{203f}'..='\u{2040}'))
    };
    valid(parts.next().unwrap_or("")) && parts.next().is_none_or(valid) && parts.next().is_none()
}
fn name_start(character: char) -> bool {
    matches!(character, '_' | 'A'..='Z' | 'a'..='z' | '\u{c0}'..='\u{d6}' | '\u{d8}'..='\u{f6}' | '\u{f8}'..='\u{2ff}' | '\u{370}'..='\u{37d}' | '\u{37f}'..='\u{1fff}' | '\u{200c}'..='\u{200d}' | '\u{2070}'..='\u{218f}' | '\u{2c00}'..='\u{2fef}' | '\u{3001}'..='\u{d7ff}' | '\u{f900}'..='\u{fdcf}' | '\u{fdf0}'..='\u{fffd}' | '\u{10000}'..='\u{effff}')
}
fn valid_chars(text: &str, xml11: bool) -> bool {
    text.chars().all(|character| matches!(character, '\t' | '\r' | '\n' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}') || xml11 && matches!(character, '\u{1}'..='\u{1f}'))
}
fn valid_literal(text: &str, xml11: bool) -> bool {
    valid_chars(text, xml11) && (!xml11 || !text.chars().any(|character| matches!(character, '\u{1}'..='\u{8}' | '\u{b}'..='\u{c}' | '\u{e}'..='\u{1f}' | '\u{7f}'..='\u{84}' | '\u{86}'..='\u{9f}')))
}
fn normalize_attribute(text: &str, xml11: bool) -> Result<String> {
    if text.contains('<') || !valid_literal(text, xml11) { return Err(invalid("Azure federation XML attribute is invalid")); }
    let mut characters = text.chars().peekable();
    let mut output = String::new();
    while let Some(character) = characters.next() {
        match character {
            '\r' => { if characters.peek() == Some(&'\n') || xml11 && characters.peek() == Some(&'\u{85}') { characters.next(); } output.push(' '); }
            '\n' | '\t' => output.push(' '),
            '\u{85}' | '\u{2028}' if xml11 => output.push(' '),
            character => output.push(character),
        }
    }
    Ok(output)
}
fn escape(text: &str, attribute: bool) -> String {
    let mut output = String::new();
    for character in text.chars() { match character {
        '&' => output.push_str("&amp;"), '<' => output.push_str("&lt;"), '>' => output.push_str("&gt;"),
        '"' if attribute => output.push_str("&quot;"), '\t' if attribute => output.push_str("&#9;"), '\n' if attribute => output.push_str("&#10;"), '\r' => output.push_str("&#13;"),
        character if matches!(character, '\u{1}'..='\u{8}' | '\u{b}'..='\u{c}' | '\u{e}'..='\u{1f}' | '\u{7f}'..='\u{9f}') => { use std::fmt::Write; write!(output, "&#{};", character as u32).expect("string formatting"); }
        character => output.push(character),
    } }
    output
}
