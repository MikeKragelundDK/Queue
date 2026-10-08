import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// NB: ingen proxy her — Quarkus (:8080) er altid indgangen i dev, og en
// /api-proxy tilbage mod :8080 skaber et uendeligt proxy-loop med Quinoa.
export default defineConfig({
  plugins: [svelte()]
})
