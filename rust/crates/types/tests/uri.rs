use rtdb_types::UriParts;

#[test]
fn uri_components_preserve_spelling_and_absent_versus_empty_values() {
    let uri = UriParts::parse("HtTp://user%3Apass@EXAMPLE.com:2147483647/a/../b%20c?x=1+2#").unwrap();
    assert_eq!(uri.scheme(),Some("HtTp"));
    assert_eq!(uri.user_info().as_deref(),Some("user:pass"));
    assert_eq!(uri.host(),Some("EXAMPLE.com"));
    assert_eq!(uri.port(),Some(i32::MAX));
    assert_eq!(uri.path().as_deref(),Some("/a/../b c"));
    assert_eq!(uri.query().as_deref(),Some("x=1+2"));
    assert_eq!(uri.fragment().as_deref(),Some(""));
    for text in ["", "#", "?", "?#", "http://example.com", "//#"] {
        assert_eq!(UriParts::parse(text).unwrap().path().as_deref(),Some(""),"{text}");
    }
    let uri = UriParts::parse("mailto:user@example.com?subject=x#fragment").unwrap();
    assert_eq!(uri.path(),None);
    assert_eq!(uri.query(),None);
    assert_eq!(uri.host(),None);
    assert_eq!(uri.fragment().as_deref(),Some("fragment"));
    assert_eq!(UriParts::parse("file:///tmp/foo").unwrap().path().as_deref(),Some("/tmp/foo"));
    assert_eq!(UriParts::parse("../é/😀").unwrap().scheme(),None);
}

#[test]
fn registry_authorities_clear_all_server_components() {
    for text in ["http://user@bad_host:80", "http://user@host:abc", "http://user@host:2147483648", "http://user@", "http://:80", "http://é.example", "http://256.1.2.3", "http://host.123"] {
        let uri = UriParts::parse(text).unwrap();
        assert_eq!((uri.user_info(),uri.host(),uri.port()),(None,None,None),"{text}");
    }
    for (text,host,port) in [("//123","123",None),("//123.","123.",None),("//000192.168.0001.1:0","000192.168.0001.1",Some(0)),("//a-b.example.:00080","a-b.example.",Some(80)),("//@host:","host",None)] {
        let uri = UriParts::parse(text).unwrap();
        assert_eq!((uri.host(),uri.port()),(Some(host),port),"{text}");
    }
    assert_eq!(UriParts::parse("//@host").unwrap().user_info().as_deref(),Some(""));
}

#[test]
fn raw_authority_access_distinguishes_registry_authority_and_empty_user() {
    let uri = UriParts::parse("file://bad_host/share").unwrap();
    assert_eq!(uri.raw_authority(), Some("bad_host"));
    assert_eq!(uri.host(), None);
    assert_eq!(uri.raw_user_info(), None);
    let uri = UriParts::parse("file://@box/share").unwrap();
    assert_eq!(uri.raw_authority(), Some("@box"));
    assert_eq!(uri.raw_user_info(), Some(""));
    assert_eq!(UriParts::parse("file:///C:/dir").unwrap().raw_authority(), None);
}

#[test]
fn uri_ipv6_validation_retains_brackets_and_java_scope_spelling() {
    for host in ["[::]", "[::1]", "[2001:db8:0:1:2:3:4:5]", "[::ffff:000192.168.1.1]", "[fe80::1%eth0]", "[fe80::1%25eth0]", "[fe80::1%scope_1.2]"] {
        let text = format!("http://{host}:123/");
        let uri = UriParts::parse(&text).unwrap();
        assert_eq!(uri.host(),Some(host));
        assert_eq!(uri.port(),Some(123));
    }
    for host in ["[]", "[v1.a]", "[1:2:3]", "[1:2:3:4:5:6:7:8:9]", "[1:2:3:4:5:6:7::8]", "[1::2::3]", "[::ffff:256.1.1.1]", "[::1%]", "[::1%a-b]", "[::1%é]", "[::1", "[::1]:2147483648", "[::1]:x"] {
        assert!(UriParts::parse(&format!("http://{host}")).is_err(),"{host}");
    }
}

#[test]
fn uri_validation_checks_java_character_classes_and_percent_pairs() {
    for text in ["a:", "a:#f", "//", "http://", ":relative", "1:relative", "a_b:opaque", "a b", "a\\b", "a|b", "a[b]", "a#b#c", "http://host/%", "http://host/%0", "http://host/%0g", "http://u%zz@host", "a\u{80}", "a\u{a0}", "a\u{2007}", "a\u{2028}", "a\u{3000}"] {
        assert!(UriParts::parse(text).is_err(),"{text:?}");
    }
    for text in ["a:b:c", "a:?", "a:a[b]", "//?", "//#", "a\u{200b}", "a%85"] {
        assert!(UriParts::parse(text).is_ok(),"{text:?}");
    }
}

#[test]
fn uri_percent_decoding_uses_java_utf8_replacement_groups() {
    for (encoded,decoded) in [
        ("%C3%A9","é"),("%F0%9F%98%80","😀"),("%00","\0"),("%ED%A0%80","�"),("%ED%BF%BF","�"),
        ("%C0%AF","��"),("%E0%80%80","���"),("%F4%90%80%80","����"),("%F5%80%80%80","����"),
        ("%E2%82","�"),("%E2%82%41","�A"),("%E2%28%A1","�(�"),("%F0%90%80%41","�A"),
        ("%E2x%82","�x�"),("a+b","a+b"),("[%41]","[A]"),
    ] {
        let text = format!("?{encoded}#{encoded}");
        let uri = UriParts::parse(&text).unwrap();
        assert_eq!(uri.query().as_deref(),Some(decoded),"{encoded}");
        assert_eq!(uri.fragment().as_deref(),Some(decoded),"{encoded}");
    }
}

#[test]
fn uri_object_equality_folds_only_scheme_host_and_escape_hex_case() {
    for (left,right,equal) in [
        ("HTTP://User%3aPass@EXAMPLE.com:00080/a%2fb?q=%ab#%cd","http://User%3APass@example.COM:80/a%2Fb?q=%AB#%CD",true),
        ("http://host:","http://HOST",true),
        ("HTTP://[FE80::A%ETH0]/","http://[fe80::a%eth0]/",true),
        ("MAILTO:x%ab?y#%cd","mailto:x%AB?y#%CD",true),
        ("//bad_host/%ab","//bad_host/%AB",true),
        ("//bad_host","//BAD_HOST",false),
        ("http://User@host","http://user@host",false),
        ("http://host/a","http://host/A",false),
        ("http://host/%41","http://host/A",false),
        ("http://host/a/../b","http://host/b",false),
        ("http://host","http://host?",false),
        ("http://host","http://host#",false),
        ("http://host","http://host:80",false),
        ("s:a","s:/a",false),
        ("/e\u{301}","/é",false),
        ("/","///",true),
    ] {
        let left = UriParts::parse(left).unwrap();
        let right = UriParts::parse(right).unwrap();
        assert_eq!(left.equivalent(&right),equal,"{left:?}, {right:?}");
        assert_eq!(right.equivalent(&left),equal,"{left:?}, {right:?}");
    }
}

#[test]
fn uri_ascii_string_normalizes_unicode_without_rewriting_original_ascii_components() {
    for (source,expected) in [("HTTP://host/e\u{301}","HTTP://host/%C3%A9"),("/\u{1100}\u{1161}","/%EA%B0%80"),("/ﬃ","/%EF%AC%83"),("/%65%CC%81","/%65%CC%81")] {
        let uri = UriParts::parse(source).unwrap();
        let bound = uri.ascii_allocation_bound().unwrap();
        let actual = uri.to_ascii_string();
        assert_eq!(actual,expected);
        assert!(actual.capacity() <= bound);
    }
}

#[test]
fn java_uri_object_hash_uses_utf16_and_component_case_rules() {
    for (uri,hash) in [("abc",96354),("s:a",14702),("😀",1772899),("%af",37642)] {
        assert_eq!(rtdb_types::UriParts::parse(uri).unwrap().java_hash_code(),hash);
    }
    for (left,right) in [("HTTP://Example.COM/a%2fb?q=%af#%bc","http://example.com/a%2Fb?q=%AF#%BC"),("s:a%af","S:a%AF"),("//registry:invalid/a%af","//registry:invalid/a%AF")] {
        let left = rtdb_types::UriParts::parse(left).unwrap();let right = rtdb_types::UriParts::parse(right).unwrap();
        assert!(left.equivalent(&right));assert_eq!(left.java_hash_code(),right.java_hash_code());
    }
    assert_ne!(rtdb_types::UriParts::parse("a/../b").unwrap().java_hash_code(),rtdb_types::UriParts::parse("b").unwrap().java_hash_code());
}
