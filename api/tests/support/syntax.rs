#![allow(dead_code)]

use std::collections::BTreeMap;
use syn::visit::Visit;

pub fn mask(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut masked = bytes.to_vec();
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    masked[index] = b' ';
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = mask_comment(bytes, &mut masked, index);
            }
            b'r' if matches!(bytes.get(index + 1), Some(b'#' | b'"')) => {
                index = mask_raw(bytes, &mut masked, index);
            }
            b'"' => index = mask_string(bytes, &mut masked, index),
            b'\'' => index = mask_char(bytes, &mut masked, index),
            _ => index += 1,
        }
    }

    String::from_utf8(masked).expect("masking only replaces whole bytes with spaces")
}

pub fn production(source: &str) -> String {
    let mut source = mask(source);
    let mut offset = 0;

    while let Some(relative) = source[offset..].find("#[") {
        let start = offset + relative;
        let bracket = start + 1;
        let close = delimiter(&source, bracket, b'[', b']');
        let attribute = &source[start..=close];
        if cfg_attr(attribute) {
            panic!("unsupported cfg_attr attribute: {attribute}");
        }
        if test_cfg(attribute) {
            let end = item_end(&source, close + 1);
            source.replace_range(start..end, &" ".repeat(end - start));
            offset = start;
        } else {
            offset = close + 1;
        }
    }

    source
}

pub fn test_cfg(attribute: &str) -> bool {
    if attribute_name(attribute) != "cfg" {
        return false;
    }
    let inner = attribute
        .trim()
        .strip_prefix("#![")
        .or_else(|| attribute.trim().strip_prefix("#["))
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or_else(|| panic!("invalid attribute: {attribute}"));
    let meta = syn::parse_str::<syn::Meta>(inner)
        .unwrap_or_else(|error| panic!("invalid attribute {attribute}: {error}"));
    let predicate = match meta {
        syn::Meta::List(list) => syn::parse2::<syn::Meta>(list.tokens)
            .unwrap_or_else(|error| panic!("invalid cfg attribute {attribute}: {error}")),
        _ => panic!("invalid cfg attribute: {attribute}"),
    };
    requires_test(&predicate)
}

pub fn cfg_attr(attribute: &str) -> bool {
    attribute_name(attribute) == "cfg_attr"
}

pub fn function_calls(source: &str) -> BTreeMap<String, Vec<String>> {
    let file =
        syn::parse_file(source).unwrap_or_else(|error| panic!("invalid Rust source: {error}"));
    let mut functions = BTreeMap::new();
    collect_items(&file.items, &mut functions);
    functions
}

pub fn use_paths(source: &str) -> BTreeMap<String, String> {
    let file =
        syn::parse_file(source).unwrap_or_else(|error| panic!("invalid Rust source: {error}"));
    let mut imports = BTreeMap::new();
    for item in file.items {
        if let syn::Item::Use(item_use) = item
            && !attrs_test(&item_use.attrs)
        {
            collect_use(&item_use.tree, Vec::new(), &mut imports);
        }
    }
    imports
}

pub fn delimiter(source: &str, open: usize, start: u8, end: u8) -> usize {
    let bytes = source.as_bytes();
    assert_eq!(
        bytes.get(open),
        Some(&start),
        "expected opening delimiter at byte {open}"
    );
    let mut depth = 0;

    for (index, byte) in bytes.iter().enumerate().skip(open) {
        if *byte == start {
            depth += 1;
        } else if *byte == end {
            depth -= 1;
            if depth == 0 {
                return index;
            }
        }
    }

    panic!("unbalanced delimiter at byte {open}");
}

pub fn skip_space(source: &str, mut pos: usize) -> usize {
    while source
        .as_bytes()
        .get(pos)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        pos += 1;
    }
    pos
}

pub fn ident(source: &str, start: usize) -> String {
    let mut end = start;
    while source
        .as_bytes()
        .get(end)
        .is_some_and(|byte| ident_byte(*byte))
    {
        end += 1;
    }
    source[start..end].to_owned()
}

pub fn read_ident(source: &str, start: usize) -> (String, usize) {
    let name = ident(source, start);
    let end = start + name.len();
    (name, end)
}

pub fn ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub fn occurrences(source: &str, needle: &str) -> Vec<usize> {
    source.match_indices(needle).map(|(at, _)| at).collect()
}

fn requires_test(meta: &syn::Meta) -> bool {
    if let syn::Meta::Path(path) = meta {
        return path.is_ident("test");
    }
    let syn::Meta::List(list) = meta else {
        return false;
    };
    let nested = list
        .parse_args_with(syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
        .unwrap_or_else(|error| panic!("invalid cfg predicate: {error}"));
    if list.path.is_ident("all") {
        nested.iter().any(requires_test)
    } else if list.path.is_ident("any") {
        !nested.is_empty() && nested.iter().all(requires_test)
    } else {
        false
    }
}

fn attribute_name(attribute: &str) -> &str {
    let inner = attribute
        .trim()
        .strip_prefix("#![")
        .or_else(|| attribute.trim().strip_prefix("#["))
        .unwrap_or("")
        .trim_start();
    let end = inner
        .find(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .unwrap_or(inner.len());
    &inner[..end]
}

fn collect_items(items: &[syn::Item], functions: &mut BTreeMap<String, Vec<String>>) {
    for item in items {
        match item {
            syn::Item::Fn(function) if !attrs_test(&function.attrs) => {
                collect_function(&function.sig.ident.to_string(), &function.block, functions)
            }
            syn::Item::Impl(item_impl) if !attrs_test(&item_impl.attrs) => {
                for item in &item_impl.items {
                    if let syn::ImplItem::Fn(function) = item
                        && !attrs_test(&function.attrs)
                        && has_attribute(&function.attrs, "tool")
                    {
                        collect_function(
                            &function.sig.ident.to_string(),
                            &function.block,
                            functions,
                        );
                    }
                }
            }
            syn::Item::Mod(module) if module.content.is_some() && !attrs_test(&module.attrs) => {
                panic!("inline module {} is unsupported", module.ident)
            }
            _ => {}
        }
    }
}

fn collect_use(
    tree: &syn::UseTree,
    mut prefix: Vec<String>,
    imports: &mut BTreeMap<String, String>,
) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_use(&path.tree, prefix, imports);
        }
        syn::UseTree::Name(name) => {
            prefix.push(name.ident.to_string());
            let local = name.ident.to_string();
            imports.insert(local, prefix.join("::"));
        }
        syn::UseTree::Rename(rename) => {
            prefix.push(rename.ident.to_string());
            imports.insert(rename.rename.to_string(), prefix.join("::"));
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                collect_use(item, prefix.clone(), imports);
            }
        }
        syn::UseTree::Glob(_) => panic!("glob import requires explicit resolution"),
    }
}

fn has_attribute(attributes: &[syn::Attribute], name: &str) -> bool {
    attributes
        .iter()
        .any(|attribute| attribute.path().is_ident(name))
}

fn attrs_test(attributes: &[syn::Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        if attribute.path().is_ident("cfg_attr") {
            panic!("unsupported cfg_attr attribute")
        }
        attribute.path().is_ident("cfg")
            && requires_test(
                &attribute
                    .parse_args::<syn::Meta>()
                    .unwrap_or_else(|error| panic!("invalid cfg attribute: {error}")),
            )
    })
}

fn collect_function(name: &str, block: &syn::Block, functions: &mut BTreeMap<String, Vec<String>>) {
    assert!(
        !functions.contains_key(name),
        "duplicate function name {name} requires scoped resolution"
    );
    let mut visitor = CallVisitor::default();
    visitor.visit_block(block);
    functions.insert(name.to_owned(), visitor.calls);
}

#[derive(Default)]
struct CallVisitor {
    calls: Vec<String>,
}

impl<'ast> Visit<'ast> for CallVisitor {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = call.func.as_ref() {
            self.calls.push(
                path.path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::"),
            );
        }
        syn::visit::visit_expr_call(self, call);
    }

    fn visit_item_fn(&mut self, function: &'ast syn::ItemFn) {
        panic!(
            "nested function {} requires scoped resolution",
            function.sig.ident
        );
    }
}

fn item_end(source: &str, from: usize) -> usize {
    let from = skip_space(source, from);
    let tail = &source[from..];
    let semi = tail.find(';');
    let brace = tail.find('{');

    match (semi, brace) {
        (Some(semi), Some(brace)) if semi < brace => from + semi + 1,
        (_, Some(brace)) => delimiter(source, from + brace, b'{', b'}') + 1,
        (Some(semi), None) => from + semi + 1,
        (None, None) => source.len(),
    }
}

fn mask_comment(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    let mut index = from;
    let mut depth = 0;

    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            depth += 1;
        } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            depth -= 1;
            masked[index] = b' ';
            masked[index + 1] = b' ';
            index += 2;
            if depth == 0 {
                return index;
            }
            continue;
        }
        if bytes[index] != b'\n' {
            masked[index] = b' ';
        }
        index += 1;
    }

    index
}

fn mask_raw(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    let mut index = from + 1;
    while bytes.get(index) == Some(&b'#') {
        index += 1;
    }
    if bytes.get(index) != Some(&b'"') {
        return from + 1;
    }
    let hashes = index - from - 1;
    masked[from..=index].fill(b' ');
    index += 1;

    while index < bytes.len() {
        if bytes[index] == b'"' && bytes[index + 1..].iter().take(hashes).all(|at| *at == b'#') {
            masked[index..=index + hashes].fill(b' ');
            return index + hashes + 1;
        }
        if bytes[index] != b'\n' {
            masked[index] = b' ';
        }
        index += 1;
    }

    index
}

fn mask_string(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    masked[from] = b' ';
    let mut index = from + 1;

    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                masked[index] = b' ';
                if bytes.get(index + 1).is_some_and(|byte| *byte != b'\n') {
                    masked[index + 1] = b' ';
                }
                index += 2;
            }
            b'"' => {
                masked[index] = b' ';
                return index + 1;
            }
            b'\n' => index += 1,
            _ => {
                masked[index] = b' ';
                index += 1;
            }
        }
    }

    index
}

fn mask_char(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    let end = if bytes.get(from + 1) == Some(&b'\\') {
        bytes[from + 2..]
            .iter()
            .position(|byte| *byte == b'\'')
            .map(|at| from + 2 + at)
    } else {
        (bytes.get(from + 2) == Some(&b'\'')).then_some(from + 2)
    };

    match end {
        Some(end) => {
            masked[from..=end].fill(b' ');
            end + 1
        }
        None => from + 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_syntax_masked() {
        let source = "r##\"call()\"## /* outer /* call() */ */ call()";
        assert!(mask(source).ends_with(" call()"));
    }

    #[test]
    fn cfg_terms_parsed() {
        assert!(test_cfg("#[cfg(all(test, unix))]"));
        assert!(!test_cfg("#[cfg(all(test_mode, unix))]"));
        assert!(!test_cfg("#[cfg(any(test, unix))]"));
    }

    #[test]
    fn valued_attributes_allowed() {
        let source = "#[path = \"child.rs\"]\nmod child;\n#[doc = \"text\"]\nfn item() {}";
        let production = production(source);
        assert!(production.contains("mod child"));
        assert!(production.contains("fn item"));
    }

    #[test]
    fn spaced_calls_parsed() {
        let calls = function_calls("fn outer() { crate :: auth :: check \n (); }");
        assert_eq!(calls["outer"], ["crate::auth::check"]);
    }

    #[test]
    #[should_panic(expected = "nested function")]
    fn nested_function_rejected() {
        let _ = function_calls("fn outer() { fn inner() {} inner(); }");
    }
}
