use super::*;

fn mex(entries: &[(&str, &str, Version)], security: &str, supporting: &str) -> String {
    let mut body = format!("<w:definitions xmlns:w='{WSDL}' xmlns:p='{POLICY}' xmlns:sp='{security}' xmlns:wsu='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd' xmlns:soap='{SOAP_BINDING}' xmlns:a='{ADDRESSING}'>");
    for (id, _, _) in entries {
        body.push_str(&format!("<p:Policy wsu:Id='{id}'><p:ExactlyOne><p:All><sp:TransportBinding/><sp:{supporting}><p:Policy><sp:UsernameToken><p:Policy><sp:WssUsernameToken10/></p:Policy></sp:UsernameToken></p:Policy></sp:{supporting}></p:All></p:ExactlyOne></p:Policy>"));
    }
    for (id, _, version) in entries {
        body.push_str(&format!("<w:binding name='{id}'><p:PolicyReference URI='#{id}'/><soap:binding transport='http://schemas.xmlsoap.org/soap/http'/><w:operation><soap:operation soapAction='{}'/></w:operation></w:binding>", version.action()));
    }
    body.push_str("<w:service>");
    for (id, endpoint, _) in entries {
        body.push_str(&format!("<w:port binding='w:{id}'><a:EndpointReference><a:Address>{endpoint}</a:Address></a:EndpointReference></w:port>"));
    }
    body.push_str("</w:service></w:definitions>");
    body
}

fn response(version: Version, token_type: &str, body: &str) -> String {
    let namespace = version.namespace();
    let token = format!("<t:RequestSecurityTokenResponse><t:TokenType>{token_type}</t:TokenType><t:RequestedSecurityToken>{body}</t:RequestedSecurityToken></t:RequestSecurityTokenResponse>");
    let token = if version == Version::Trust13 { format!("<t:RequestSecurityTokenResponseCollection>{token}</t:RequestSecurityTokenResponseCollection>") } else { token };
    format!("<s:Envelope xmlns:s='{SOAP}' xmlns:t='{namespace}'><s:Body>{token}</s:Body></s:Envelope>")
}

#[test]
fn realm_requires_only_federated_account_type_and_retains_duplicate_coercion() {
    let realm = parse_realm(br#"{"ver":"0x1.8p0f","account_type":"Managed","account_type":"fEdErAtEd","federation_protocol":"unknown","federation_metadata_url":123,"cloud_audience_urn":true}TRAILING"#).unwrap();
    assert!(realm.federated);
    assert_eq!(realm.metadata.as_deref(), Some("123"));
    assert_eq!(realm.audience.as_deref(), Some("true"));
    for account in ["Managed", "unknown", "Federated "] {
        assert!(!parse_realm(format!("{{\"account_type\":\"{account}\"}}").as_bytes()).unwrap().federated);
    }
    assert!(!parse_realm(br#"{"account_type":"Federated","account_type":null}"#).unwrap().federated);
    assert!(parse_realm(br#"null"#).is_err());
    assert!(parse_realm(br#"{"ver":true}"#).is_err());
    assert!(parse_realm(br#"{"unknown":{"bad":1},"ver":"invalid"}"#).is_err());
}

#[test]
fn java_version_syntax_accepts_suffixes_hex_specials_and_signed_exponents() {
    for value in ["1", "+1.", "-.1f", "1E+999999999", "0x.1p-99D", "NaN", "-Infinity", "\u{1f}1.2\u{20}"] { assert!(java_float_literal(value), "{value}"); }
    for value in ["", ".", "1e", "0x1", "infinity", "Infinityf", "1_0", "0x1p.2", "\u{a0}1"] { assert!(!java_float_literal(value), "{value}"); }
}

#[test]
fn mex_prefers_trust13_and_supports_both_policy_namespaces() {
    for (security, supporting) in [(SECURITY, "SignedEncryptedSupportingTokens"), (SECURITY_2005, "SignedSupportingTokens")] {
        let text = mex(&[("new", "https://example.org/new", Version::Trust13), ("old", "https://example.org/old", Version::Trust2005)], security, supporting);
        let selected = parse_policy(&text, false, false).unwrap();
        assert_eq!(selected.version, Version::Trust13);
        assert_eq!(selected.endpoint.as_str(), "https://example.org/new");
    }
    let text = mex(&[("old", "https://example.org/old", Version::Trust2005)], SECURITY_2005, "SignedSupportingTokens");
    assert_eq!(parse_policy(&text, true, false).unwrap().version, Version::Trust2005);
}

#[test]
fn mex_namespace_mutation_changes_later_signed_encrypted_policy_selection() {
    let modern = mex(&[("modern", "https://example.org/modern", Version::Trust13)], SECURITY, "SignedEncryptedSupportingTokens");
    assert!(parse_policy(&modern, false, false).is_ok());
    assert!(parse_policy(&modern, true, false).is_err());
    let legacy = mex(&[("legacy", "https://example.org/legacy", Version::Trust13)], SECURITY_2005, "SignedEncryptedSupportingTokens");
    assert!(parse_policy(&legacy, false, false).is_err());
    assert!(parse_policy(&legacy, true, false).is_ok());
}

#[test]
fn mex_last_hashmap_policy_wins_and_duplicates_retain_position() {
    let entries = [("z", "https://example.org/z", Version::Trust13), ("a", "https://example.org/a", Version::Trust13)];
    let selected = parse_policy(&mex(&entries, SECURITY_2005, "SignedSupportingTokens"), false, false).unwrap();
    assert_eq!(selected.endpoint.as_str(), "https://example.org/z");
    let duplicate = mex(&[("same", "https://example.org/first", Version::Trust13), ("same", "https://example.org/second", Version::Trust13)], SECURITY_2005, "SignedSupportingTokens");
    assert_eq!(parse_policy(&duplicate, false, false).unwrap().endpoint.as_str(), "https://example.org/first");
}

#[test]
fn mex_requires_https_first_address_transport_and_exact_wsu_attribute_name() {
    let text = mex(&[("p", "https://example.org/trust", Version::Trust13)], SECURITY_2005, "SignedSupportingTokens");
    assert!(parse_policy(&text.replace("https://example.org/trust", "http://example.org/trust"), false, false).is_err());
    assert!(parse_policy(&text.replace("https://example.org/trust", " https://example.org/trust"), false, false).is_err());
    assert!(parse_policy(&text.replace("http://schemas.xmlsoap.org/soap/http", "urn:wrong"), false, false).is_err());
    assert!(parse_policy(&text.replace("<a:Address>", "<a:Wrong>").replace("</a:Address>", "</a:Wrong>"), false, false).is_err());
    assert!(parse_policy(&text.replace("wsu:", "utility:").replace("xmlns:wsu=", "xmlns:utility="), false, false).is_err());
    assert!(parse_policy(&text.replace("wsu:Id='p'", "wsu:Id='different'"), false, false).is_err());
}

#[test]
fn soap_versions_preserve_request_fields_escape_credentials_and_ten_minute_timestamp() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05.678Z").unwrap().with_timezone(&chrono::Utc);
    for version in [Version::Trust13, Version::Trust2005] {
        let policy = Policy { endpoint: Url::parse("https://example.org/trust").unwrap(), address: "https://example.org/trust".into(), version };
        let body = request(&policy, "user<&\"'", "pass<&\"'", Some(" "), now, "message-id", "token-id");
        assert!(body.contains("<a:messageID>urn:uuid:message-id</a:messageID>"));
        assert!(body.contains("<o:UsernameToken u:Id='uuid-token-id'>"));
        assert!(body.contains("user&lt;&amp;&quot;&apos;"));
        assert!(body.contains("pass&lt;&amp;&quot;&apos;"));
        assert!(body.contains("<u:Created>2026-01-02T03:04:05Z</u:Created><u:Expires>2026-01-02T03:14:05Z</u:Expires>"));
        assert!(body.contains(version.action()));
        assert!(body.contains("urn:federation:MicrosoftOnline"));
        assert!(Document::parse(&body).is_ok());
    }
}

#[test]
fn assertion_versions_retain_namespace_and_url_safe_base64_padding() {
    for version in [Version::Trust13, Version::Trust2005] {
        for token_type in [SAML1, "urn:oasis:names:tc:SAML:2.0:assertion", ""] {
            let value = assertion(&response(version, token_type, "<saml:Assertion xmlns:saml='urn:assertion' ID='id'>content</saml:Assertion>"), version).unwrap();
            assert_eq!(value.grant_type.ends_with("saml1_1-bearer"), token_type == SAML1);
            let decoded = String::from_utf8(URL_SAFE.decode(&value.encoded).unwrap()).unwrap();
            assert_eq!(decoded, "<saml:Assertion xmlns:saml=\"urn:assertion\" ID=\"id\">content</saml:Assertion>");
            assert_eq!(URL_SAFE.encode(decoded.as_bytes()), value.encoded);
        }
    }
}

#[test]
fn assertion_serialization_retains_cumulative_sibling_output() {
    let value = assertion(&response(Version::Trust13, SAML1, "<one/><two/>"), Version::Trust13).unwrap();
    assert_eq!(String::from_utf8(URL_SAFE.decode(value.encoded).unwrap()).unwrap(), "<one/><one/><two/>");
}

#[test]
fn assertion_chooses_first_nonempty_token_and_rejects_multiple_requested_nodes() {
    let mut text = response(Version::Trust13, SAML1, "");
    text = text.replace("</t:RequestSecurityTokenResponseCollection>", "<t:RequestSecurityTokenResponse><t:TokenType>different</t:TokenType><t:RequestedSecurityToken><second/></t:RequestedSecurityToken></t:RequestSecurityTokenResponse></t:RequestSecurityTokenResponseCollection>");
    let token = assertion(&text, Version::Trust13).unwrap();
    assert!(token.grant_type.ends_with("saml2-bearer"));
    assert_eq!(URL_SAFE.decode(token.encoded).unwrap(), b"<second/>");
    let duplicate = response(Version::Trust13, SAML1, "<first/>").replace("</t:RequestedSecurityToken>", "</t:RequestedSecurityToken><t:RequestedSecurityToken><second/></t:RequestedSecurityToken>");
    assert!(assertion(&duplicate, Version::Trust13).is_err());
    assert!(assertion(&response(Version::Trust13, SAML1, ""), Version::Trust13).is_err());
}

#[test]
fn soap_fault_codes_reject_redacted_but_reason_without_subcode_does_not_short_circuit() {
    let text = response(Version::Trust13, SAML1, "<assertion/>");
    let reason = "<s:Fault><s:Reason><s:Text>private failure</s:Text></s:Reason></s:Fault>";
    assert!(assertion(&text.replace("<s:Body>", &format!("<s:Body>{reason}")), Version::Trust13).is_ok());
    let fault = "<s:Fault><s:Code><s:Subcode><s:Value>s:private-secret</s:Value></s:Subcode></s:Code></s:Fault>";
    let error = assertion(&text.replace("<s:Body>", &format!("<s:Body>{fault}")), Version::Trust13).err().unwrap();
    assert!(!error.to_string().contains("private-secret"));
}

#[test]
fn xml_rejects_external_entities_oversized_or_invalid_graphs_and_resolves_namespaces() {
    for text in ["<!DOCTYPE d SYSTEM 'file:///private'><d/>", "<!DOCTYPE d [<!ENTITY x 'secret'>]><d>&x;</d>", "<a/><b/>", "<a><b></a>", "<a>&unknown;</a>", "<a>&#0;</a>", "<unknown:a/>"] { assert!(Document::parse(text).is_err(), "{text}"); }
    assert!(Document::parse(&"<a>".repeat(257)).is_err());
    assert!(Document::parse(&format!("<a>{}</a>", "x".repeat(MAX_RESPONSE_BYTES))).is_err());
    let document = Document::parse("<a xmlns='urn:root' xmlns:p='urn:p'><p:b x='&quot;'>&amp;<![CDATA[<]]>text</p:b></a>").unwrap();
    let root = document.descendants("urn:root", "a")[0];
    let child = document.children(root, "urn:p", "b")[0];
    assert_eq!(document.text_content(child), "&<text");
    assert_eq!(document.attribute(child, "x"), Some("\""));
}

#[test]
fn xml_version_controls_line_endings_and_attribute_references() {
    let xml10 = Document::parse("<a x='line\r\nnext&#10;line'>one\u{85}two</a>").unwrap();
    let node = xml10.descendants("", "a")[0];
    assert_eq!(xml10.text_content(node), "one\u{85}two");
    assert_eq!(xml10.attribute(node, "x"), Some("line next\nline"));
    let xml11 = Document::parse("<?xml version='1.1'?><a>one\u{85}two&#1;</a>").unwrap();
    assert_eq!(xml11.text_content(xml11.descendants("", "a")[0]), "one\ntwo\u{1}");
    assert!(Document::parse("<?xml version='1.1'?><a>\u{1}</a>").is_err());
}

#[test]
fn xml_rejects_invalid_names_attributes_and_misplaced_declarations() {
    for text in ["<1bad/>", "<a:bad:extra xmlns:a='urn:a'/>", "<a x='<bad'/>", " <?xml version='1.0'?><a/>", "<?xml version='1.0'?><?xml version='1.0'?><a/>"] { assert!(Document::parse(text).is_err(), "{text}"); }
    let document = Document::parse("<root><a xmlnsExtra='retained'/></root>").unwrap();
    assert_eq!(document.inner_xml(document.descendants("", "root")[0]).unwrap(), "<a xmlnsExtra=\"retained\"/>");
}

#[test]
fn xml_namespace_expansion_has_a_retained_byte_limit() {
    let namespace = "x".repeat(128 * 1024);
    let text = format!("<root xmlns:p='{namespace}'>{}</root>", "<p:child/>".repeat(80));
    assert!(text.len() < MAX_RESPONSE_BYTES);
    assert!(Document::parse(&text).is_err());
}
