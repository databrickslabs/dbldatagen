const path = require('path');
const express = require('express');

module.exports = function () {
  return {
    name: 'static-sphinx',
    configureWebpack() {
      return {
        devServer: {
          setupMiddlewares(middlewares, devServer) {
            // Serve Sphinx HTML before the SPA fallback catches it
            devServer.app.use(
              '/dbldatagen/public_docs',
              express.static(path.resolve(__dirname, '../static/public_docs'))
            );
            return middlewares;
          },
        },
        watchOptions: {
          ignored: [
            '**/node_modules/**',
            '**/public_docs/**',
            '**/build/**',
            '**/.docusaurus/**',
          ],
        },
      };
    },
  };
};
