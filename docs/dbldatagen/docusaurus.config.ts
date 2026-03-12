import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Databricks Labs Data Generator',
  tagline: 'Generate large volumes of synthetic data from Databricks',
  favicon: 'img/favicon.png',

  future: {
    v4: true,
  },

  url: 'https://databrickslabs.github.io',
  baseUrl: '/dbldatagen/',

  organizationName: 'databrickslabs',
  projectName: 'dbldatagen',

  onBrokenLinks: 'warn',

  markdown: {
    format: 'md',
    hooks: {
      onBrokenMarkdownLinks: 'warn',
      onBrokenMarkdownImages: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        indexBlog: false,
      },
    ],
  ],

  plugins: [
    './plugins/static-sphinx',
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          path: './docs',
          sidebarPath: './sidebars.ts',
          exclude: [],
          editUrl:
            'https://github.com/databrickslabs/dbldatagen/tree/master/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'dbldatagen',
      items: [
        {
          type: 'html',
          position: 'left',
          value: '<a href="/dbldatagen/public_docs/" target="_blank" style="color:var(--ifm-navbar-link-color);font-weight:var(--ifm-font-weight-semibold)">V0 Docs</a>',
        },
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'V1 Docs (Preview)',
        },
        {
          to: '/docs/v1/guides/basic',
          position: 'left',
          label: 'Guides',
        },
        {
          href: 'https://github.com/databrickslabs/dbldatagen',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'V0 Docs & API',
              href: '/dbldatagen/public_docs/',
            },
            {
              label: 'V1 Docs',
              to: '/docs/v1/getting-started/overview',
            },
            {
              label: 'Guides & Examples',
              to: '/docs/v1/guides/basic',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub Issues',
              href: 'https://github.com/databrickslabs/dbldatagen/issues',
            },
            {
              label: 'PyPI',
              href: 'https://pypi.org/project/dbldatagen/',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Databricks Labs',
              href: 'https://github.com/databrickslabs',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Databricks, Inc. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['python', 'bash'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
