# Applet guide

This starter is the base for an Aruna applet: a static Vue site that Aruna builds, previews and,
after approval, serves on its own host. The files a creator previews are the exact files that are
approved and served later. Nothing is rebuilt at approval.

## Build

- `npm run build` writes the site to `dist/`. The Aruna builder runs the same build.
- The builder sets `ARUNA_PREVIEW_SOURCE_MAP=1`. The `aruna-source-map.ts` plugin then adds
  `dist/aruna-source-map.json`, which maps page elements to their source lines for the preview.
  It changes no other file. Never add `data-aruna-source` or similar attributes to the markup.
- Client-side routes work. A page load of a path without a file extension that has no file gets
  `index.html`.

## Libraries

The starter installs these libraries. They are defaults that keep applets consistent and build
without network access. They are not a limit.

| Library | Version | Use for | License |
| --- | --- | --- | --- |
| `vue` | 3.5.42 | framework | MIT |
| `vite` | 8.2.2 | bundler | MIT |
| `typescript` | 6.0.3 | language | Apache-2.0 |
| `tailwindcss` | 4.3.3 | styling | MIT |
| `reka-ui` | 2.11.0 | accessible component primitives | MIT |
| `@lucide/vue` | 1.38.0 | icons | ISC |
| `echarts` | 6.1.0 | charts | Apache-2.0 |
| `three` | 0.187.0 | 3D views | MIT |
| `@tanstack/vue-table` | 9.3.1 | tables | MIT |
| `maplibre-gl` | 6.19.0 | maps | BSD-3-Clause |
| `papaparse` | 5.7.0 | CSV parsing | MIT |

- Reach for these first. Any other package may be added when its license is not in the GPL
  family (GPL, LGPL, AGPL). Pin it in `package-lock.json`.
- A package the starter does not have is installed through the operator's npm mirror. Without a
  mirror, the build only works with the packages above.
- `@vitejs/plugin-vue` and `@tailwindcss/vite` are build plugins of the starter itself.

## Components

`src/components/ui/` holds shadcn-vue style components (`button`, `card`, `input`) built on
`reka-ui`. shadcn-vue is component source copied into the project. It is not an npm package, so
never install it. To add a component, copy its source from shadcn-vue into `src/components/ui/`
and use the local `cn` helper from `src/lib/utils.ts`. That helper joins class names. It does not
resolve conflicting Tailwind classes, so pass one class per property.

## Talking to Aruna

An applet talks only to its own origin. All Aruna calls go to `/_aruna/` on the applet host, and
the gateway adds the visitor's session. The page never sees a token.

| Route | Use |
| --- | --- |
| `/_aruna/session` | Who the visitor is and what the applet may do for them |
| `/_aruna/uploads` | Upload a visitor's file |
| `/_aruna/submissions` | Submit a form or a dataset |
| `/_aruna/runs` | Start a run or list runs |
| `/_aruna/runs/<run_id>` | Read one run |
| `/_aruna/runs/<run_id>/results` | Read a run's results |

- `src/lib/aruna.ts` has `arunaFetch` and `arunaJson` for these routes, and `signIn` and
  `signOut`. The request and response bodies are described in the node's API reference.
- A call that changes something must come from the applet page itself (`fetch` from the page does
  this). A missing session answers 401; call `signIn()` to send the visitor to sign in.

## Browser limits

The gateway sends a strict content security policy:

- Scripts, styles, fonts, images and data come only from the applet's own origin. Bundle every
  asset; do not load anything from a CDN or another host. Map tiles and fonts must be bundled.
- No inline scripts and no `eval`. Inline styles and style elements are allowed.
- `data:` and `blob:` URLs work for images and fonts, and `blob:` for workers.
- The applet can be framed only by the origins its owners allow for embedding.

## Variants for the preview

Mark alternatives the creator can compare live in the preview:

```html
<header data-aruna-part="intro" data-aruna-option="plain">...</header>
<header data-aruna-part="intro" data-aruna-option="bold" hidden>...</header>
```

- Give every option of one part the same `data-aruna-part` and its own `data-aruna-option`.
- Show the default option and add a static `hidden` attribute to every other option. Use the
  attributes in the template, not `v-if`, so the preview can switch between them.
- Visitors of the approved applet see the option without `hidden`.
