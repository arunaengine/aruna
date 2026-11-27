// Writes dist/aruna-source-map.json when ARUNA_PREVIEW_SOURCE_MAP=1: CSS selectors of template
// elements mapped to their source file and line. It never changes the built HTML, JS or CSS.
import { relative, sep } from "node:path";
import type { Plugin } from "vite";
import { parse } from "vue/compiler-sfc";

type Entry = { selector: string; file: string; line: number; column: number };
type Node = {
  type: number;
  tag?: string;
  tagType?: number;
  props?: Array<{ type: number; name: string; value?: { content: string } }>;
  loc?: { start: { line: number; column: number } };
  children?: Node[];
};

const ELEMENT = 1;
const ATTRIBUTE = 6;
const NATIVE = 0;
/** Template ancestors kept in a selector; more only makes it brittle. */
const CHAIN = 3;

export function arunaSourceMap(): Plugin {
  const enabled = process.env.ARUNA_PREVIEW_SOURCE_MAP === "1";
  const files = new Map<string, Entry[]>();
  let root = process.cwd();
  return {
    name: "aruna-source-map",
    apply: "build",
    enforce: "pre",
    configResolved(config) {
      root = config.root;
    },
    transform(code, id) {
      if (!enabled || !id.endsWith(".vue")) return null;
      const ast = parse(code, { filename: id }).descriptor.template?.ast as Node | undefined;
      if (!ast) return null;
      const file = relative(root, id).split(sep).join("/");
      const entries: Entry[] = [];
      const walk = (node: Node, chain: string[]) => {
        let next = chain;
        if (node.type === ELEMENT && node.tagType === NATIVE && node.tag && node.loc) {
          next = [...chain, selectorOf(node)].slice(-CHAIN);
          const { line, column } = node.loc.start;
          entries.push({ selector: next.join(" "), file, line, column });
        }
        for (const child of node.children ?? []) walk(child, next);
      };
      walk(ast, []);
      files.set(file, entries);
      return null;
    },
    generateBundle() {
      if (!enabled) return;
      const entries = [...files.keys()].sort().flatMap((file) => files.get(file) ?? []);
      const source = JSON.stringify({ version: 1, entries });
      this.emitFile({ type: "asset", fileName: "aruna-source-map.json", source });
    },
  };
}

/** The tag with its static id and classes, such as `section.hero.grid`. */
function selectorOf(node: Node): string {
  const value = (name: string) =>
    node.props?.find((prop) => prop.type === ATTRIBUTE && prop.name === name)?.value?.content;
  const id = value("id");
  const classes = (value("class") ?? "").split(/\s+/).filter(Boolean);
  const marks = ["data-aruna-part", "data-aruna-option"]
    .map((name) => [name, value(name)])
    .filter(([, mark]) => mark !== undefined)
    .map(([name, mark]) => `[${name}="${(mark ?? "").replace(/["\\]/g, "\\$&")}"]`);
  const tag = (node.tag ?? "") + (id ? `#${escape(id)}` : "");
  return tag + classes.map((c) => `.${escape(c)}`).join("") + marks.join("");
}

/** Escapes a CSS identifier, as Tailwind class names hold `:`, `/` and brackets. */
function escape(name: string): string {
  const escaped = name.replace(/[^\w-]/g, (c) => `\\${c}`);
  return escaped.replace(/^(-?)(\d)/, (_, dash, digit) => `${dash}\\3${digit} `);
}
