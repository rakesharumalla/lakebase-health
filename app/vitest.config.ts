import { defineConfig } from 'vitest/config';
import path from 'node:path';

export default defineConfig({
  test: {
    passWithNoTests: true,
    globals: true,
    environment: 'node',
    exclude: ['**/node_modules/**', '**/dist/**', '**/.databricks/**'],
    // AppKit UI imports echarts ESM paths that Node resolution cannot load; let Vite transform it.
    server: { deps: { inline: ['@databricks/appkit-ui', 'echarts-for-react'] } },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './client/src'),
    },
  },
});
