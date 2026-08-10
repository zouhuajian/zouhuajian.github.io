import { defineAstroPaperConfig } from "./src/types/config";

export default defineAstroPaperConfig({
  site: {
    url: "https://zouhuajian.github.io/",
    title: "Jay's Blog",
    description: "Big data storage engineer.",
    author: "Jay H. Zou",
    profile: "https://github.com/zouhuajian",
    ogImage: "default-og.jpg",
    lang: "en",
    timezone: "Asia/Shanghai",
    dir: "ltr",
  },
  posts: {
    perPage: 10,
    perIndex: 10,
    scheduledPostMargin: 15 * 60 * 1000,
  },
  features: {
    lightAndDarkMode: true,
    dynamicOgImage: false,
    showArchives: true,
    showBackButton: true,
    editPost: { enabled: false },
    search: "pagefind",
  },
  socials: [{ name: "github", url: "https://github.com/zouhuajian" }],
  shareLinks: [{ name: "x", url: "https://x.com/intent/post?url=" }],
});
