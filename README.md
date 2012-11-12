# IJS newsfeed

A clean, continuous, real-time aggregated stream of
semantically enriched news articles from RSS-enabled sites across the world.

## What it Does

The pipeline performs the following main steps:

   1. Periodically crawl a list of RSS feeds and a subset of Google News and obtain links to news articles
   2. Download the articles, taking care not to overload any of the hosting servers
   3. Parse each article to obtain
      *  Potential new RSS sources mentioned in the HTML, to be used in step (1)
      *  Cleartext version of the article body 
   4. Enrich the articles with semantic annotations. (using enrycher.ijs.si)
   5. Expose the stream of news articles to end users.

## Demo

See newsfeed.ijs.si.

## License

BSD-3; see the `LICENSE` file.