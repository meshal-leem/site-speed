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

wrangler deploy --env leem
https://edge-api-layer-leem-prod.stagingleem.workers.dev

wrangler deploy --env aura
https://edge-api-layer-aura-prod.stagingleem.workers.dev


wrangler tail --env aura --format=pretty

wrangler tail --env leem --format=pretty


npm run prewarm:decor
LANGUAGE=ar npm run prewarm:decor  


REGION=sa npm run prewarm:decor 
REGION=sa LANGUAGE=ar npm run prewarm:decor  