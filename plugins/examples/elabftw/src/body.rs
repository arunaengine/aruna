//! Experiment bodies: the canonical HTML form that proves a change cosmetic, the plain text
//! description derived from a body, and the body built from an edited description.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;

use html5ever::tendril::StrTendril;
use html5ever::tokenizer::states::RawKind;
use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};
use serde::{Deserialize, Serialize};

/// How eLabFTW stores a body: `content_type` 1 is HTML, 2 is Markdown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Html,
    Markdown,
}

impl Format {
    pub fn from_type(content_type: u64) -> Self {
        match content_type {
            2 => Self::Markdown,
            _ => Self::Html,
        }
    }

    pub fn content_type(self) -> u64 {
        match self {
            Self::Html => 1,
            Self::Markdown => 2,
        }
    }

    /// The `encodingFormat` of the dataset.
    pub fn mime(self) -> &'static str {
        match self {
            Self::Html => "text/html",
            Self::Markdown => "text/markdown",
        }
    }

    pub fn from_mime(mime: &str) -> Self {
        match mime {
            "text/markdown" => Self::Markdown,
            _ => Self::Html,
        }
    }
}

/// A body as one side stores it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Body {
    pub text: String,
    pub format: Format,
}

impl Body {
    pub fn new(text: impl Into<String>, format: Format) -> Self {
        Self {
            text: text.into(),
            format,
        }
    }

    /// The form two bodies are compared in: markup normalized for HTML, the bytes for Markdown.
    pub fn canonical(&self) -> String {
        match self.format {
            Format::Html => canonical_html(&self.text),
            Format::Markdown => self.text.clone(),
        }
    }

    /// The plain text `description` Aruna shows and searches.
    pub fn plain(&self) -> String {
        match self.format {
            Format::Html => plain_text(&self.text),
            Format::Markdown => self.text.clone(),
        }
    }

    /// The body an edited plain text description becomes; HTML loses its earlier formatting.
    pub fn from_plain(text: &str, format: Format) -> Self {
        let text = match format {
            Format::Html => paragraphs(text),
            Format::Markdown => text.to_string(),
        };
        Self { text, format }
    }
}

/// Lowercase names, sorted attributes, double quotes and one escaping, without the ignored
/// self-closing slash; text stays byte for byte.
pub fn canonical_html(html: &str) -> String {
    tokenize(html, Canonical::default()).out.into_inner()
}

/// Text of the HTML with paragraph breaks; script and style content is left out.
pub fn plain_text(html: &str) -> String {
    let text = tokenize(html, Plain::default()).out.into_inner();
    let lines: Vec<&str> = text.lines().map(str::trim).collect();
    let mut out = String::new();
    let mut blank = 0;
    for line in lines {
        if line.is_empty() {
            blank += 1;
            continue;
        }
        if !out.is_empty() {
            out.push_str(if blank > 0 { "\n\n" } else { "\n" });
        }
        out.push_str(line);
        blank = 0;
    }
    out
}

/// Each blank-line separated paragraph becomes `<p>`, each line break `<br>`.
fn paragraphs(text: &str) -> String {
    let normalized = text.replace("\r\n", "\n");
    let blocks = normalized
        .split("\n\n")
        .map(|block| block.trim_matches('\n'));
    let mut out = String::new();
    for block in blocks.filter(|block| !block.trim().is_empty()) {
        let lines: Vec<String> = block.split('\n').map(escape).collect();
        out.push_str("<p>");
        out.push_str(&lines.join("<br>"));
        out.push_str("</p>");
    }
    out
}

fn tokenize<S: TokenSink<Handle = ()>>(html: &str, sink: S) -> S {
    let tokenizer = Tokenizer::new(sink, TokenizerOpts::default());
    let input = BufferQueue::default();
    input.push_back(StrTendril::from(html));
    let _ = tokenizer.feed(&input);
    tokenizer.end();
    tokenizer.sink
}

/// The state the HTML tree builder would switch the tokenizer to after this start tag.
fn raw_state(tag: &Tag) -> TokenSinkResult<()> {
    if tag.kind != TagKind::StartTag {
        return TokenSinkResult::Continue;
    }
    match &*tag.name {
        "script" => TokenSinkResult::RawData(RawKind::ScriptData),
        "style" | "xmp" | "iframe" | "noembed" | "noframes" => {
            TokenSinkResult::RawData(RawKind::Rawtext)
        }
        "textarea" | "title" => TokenSinkResult::RawData(RawKind::Rcdata),
        "plaintext" => TokenSinkResult::Plaintext,
        _ => TokenSinkResult::Continue,
    }
}

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            other => out.push(other),
        }
    }
    out
}

#[derive(Default)]
struct Canonical {
    out: RefCell<String>,
}

impl TokenSink for Canonical {
    type Handle = ();

    fn process_token(&self, token: Token, _line: u64) -> TokenSinkResult<()> {
        let mut out = self.out.borrow_mut();
        match token {
            Token::TagToken(tag) => {
                out.push('<');
                if tag.kind == TagKind::EndTag {
                    out.push('/');
                }
                out.push_str(&tag.name.to_ascii_lowercase());
                let mut attributes: Vec<(String, &str)> = (tag.attrs.iter())
                    .map(|attr| {
                        (
                            attr.name.local.to_ascii_lowercase().to_string(),
                            &*attr.value,
                        )
                    })
                    .collect();
                attributes.sort();
                for (name, value) in attributes {
                    out.push_str(&format!(" {name}=\"{}\"", escape(value)));
                }
                out.push('>');
                return raw_state(&tag);
            }
            Token::CharacterTokens(text) => out.push_str(&escape(&text)),
            Token::NullCharacterToken => out.push('\0'),
            Token::CommentToken(text) => out.push_str(&format!("<!--{text}-->")),
            Token::DoctypeToken(doctype) => {
                let name = doctype.name.as_deref().unwrap_or_default();
                out.push_str(&format!("<!doctype {}>", name.to_ascii_lowercase()));
            }
            Token::EOFToken | Token::ParseError(_) => {}
        }
        TokenSinkResult::Continue
    }
}

#[derive(Default)]
struct Plain {
    out: RefCell<String>,
    /// Inside script or style, whose text is not shown.
    hidden: RefCell<bool>,
}

impl TokenSink for Plain {
    type Handle = ();

    fn process_token(&self, token: Token, _line: u64) -> TokenSinkResult<()> {
        let mut out = self.out.borrow_mut();
        match token {
            Token::TagToken(tag) => {
                let name = tag.name.to_ascii_lowercase().to_string();
                if matches!(name.as_str(), "script" | "style") {
                    *self.hidden.borrow_mut() = tag.kind == TagKind::StartTag;
                }
                match name.as_str() {
                    "br" => out.push('\n'),
                    "p" | "div" | "li" | "tr" | "table" | "ul" | "ol" | "blockquote" | "pre"
                    | "hr" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => out.push_str("\n\n"),
                    _ => {}
                }
                return raw_state(&tag);
            }
            Token::CharacterTokens(text) if !*self.hidden.borrow() => out.push_str(&text),
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn same(left: &str, right: &str) -> bool {
        canonical_html(left) == canonical_html(right)
    }

    #[test]
    fn cosmetic_markup() {
        assert!(same(
            r#"<P CLASS='a' id=b>x &amp; y</P>"#,
            r#"<p id="b" class="a">x &#38; y</p>"#
        ));
        assert!(same("<a href='x'>&lt;</a>", r#"<a href="x">&#60;</a>"#));
    }

    #[test]
    fn content_changes() {
        let base = r#"<p style="white-space: pre-wrap">a  b <a href="x">l</a></p>"#;
        for changed in [
            r#"<p style="white-space: pre-wrap">a b <a href="x">l</a></p>"#,
            r#"<p style="white-space: pre-wrap">a  b <a href="y">l</a></p>"#,
            r#"<p style="white-space: normal">a  b <a href="x">l</a></p>"#,
            r#"<p style="white-space: pre-wrap"><b>a  b</b> <a href="x">l</a></p>"#,
            r#"<p style="white-space: pre-wrap">a  b <a href="x">l</a></p> "#,
            r#"<p style="white-space: pre-wrap">a  b <img src="x"><a href="x">l</a></p>"#,
        ] {
            assert!(!same(base, changed), "{changed}");
        }
        // No Unicode normalization: the composed and decomposed forms differ.
        assert!(!same("<p>\u{e9}</p>", "<p>e\u{301}</p>"));
    }

    #[test]
    fn markdown_bytes() {
        let body = Body::new("# T\n\n```\ncode  \n```\n", Format::Markdown);
        assert_eq!(body.canonical(), body.text);
        let decomposed = Body::new("# T\n\ne\u{301}", Format::Markdown);
        assert_ne!(
            Body::new("# T\n\n\u{e9}", Format::Markdown).canonical(),
            decomposed.canonical()
        );
    }

    #[test]
    fn raw_text_kept() {
        let html = "<style>p > a { color: red }</style><p>x</p>";
        assert_eq!(
            canonical_html(html),
            "<style>p &gt; a { color: red }</style><p>x</p>"
        );
        assert_eq!(plain_text(html), "x");
    }

    #[test]
    fn plain_description() {
        let html = "<h1>Title</h1><p>One &amp; two<br>three</p><ul><li>a</li><li>b</li></ul>";
        assert_eq!(plain_text(html), "Title\n\nOne & two\nthree\n\na\n\nb");
    }

    #[test]
    fn description_bodies() {
        let html = Body::from_plain("a < b & \"c\"\nnext\n\nsecond", Format::Html);
        assert_eq!(
            html.text,
            "<p>a &lt; b &amp; &quot;c&quot;<br>next</p><p>second</p>"
        );
        assert_eq!(html.plain(), "a < b & \"c\"\nnext\n\nsecond");
        let markdown = Body::from_plain("*a*\n\nb", Format::Markdown);
        assert_eq!(markdown.text, "*a*\n\nb");
    }
}
