nvm install 20
nvm use 20

npm install -g wrangler
wrangler --version

site-speed-edge-api-layer/
├── wrangler.toml
├── package.json        (optional but recommended)
├── tsconfig.json       (optional)
└── src/
    └── index.ts        (Cloudflare Worker logic)

wrangler dev

wrangler login
