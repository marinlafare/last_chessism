# Frontend source map

`App.jsx` owns authentication and route selection. Route entry files live in `pages/`.
Small pages stay self-contained; pages with substantial state or markup use a feature folder:

- `pages/games/`: Games controller, view, charts, API adapters, and transformations.
- `pages/positions/`: analysis-job controller, view, progress components, and job helpers.
- `components/layout/`: application shell shared by routes.
- `components/home/`: Home-page sections.
- `services/`: shared HTTP, job, and status access.
- `utils/`: presentation-independent helpers.
- `styles/`: ordered CSS sections; `index.css` documents their cascade order.

For complex pages, keep network/state coordination in `use*Page.js`, markup in `*View.jsx`,
and pure helpers or small page-specific components beside them. `npm run check:source-size`
enforces the 1,000-line limit for JavaScript, TypeScript, JSX, TSX, and CSS source files.
