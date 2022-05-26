# Welcome to the New Help site

## How to add a new Article

### Add content

Copy the [template](link to the TEMPLATE.md) file and rename it with the name of the article title with dashes (i.e. [Everything-about-SmartScan.md](link)) and put the new file inside of the respective hub folder ([send-money](link) or [request-money](link)) or sub-folder. The title will be rendered automatically according to the filename (the dashes will be removed in the content).

The sections of the article will be filled and nested in automatically in the LHN, just ensure to use the [heading markdown tags](https://www.markdownguide.org/cheat-sheet/) correctly.

### Add a new entry in the LHN and hub page

In order to add a new article entry in the LHN, update the file [main-navigation-tree.html](link) by adding a new entry like `<li><a href="relative-file-path">Article Title</li>`, where "relative-file-path" will be the path of the file name without the extension file, i.e. "request-money/request-money/Everything-about-SmartScan".

Also update the respective hub page ([send-money/index.html](link) or [request-money/index.html](link)) by adding a new Card element with the article page and a link to the relative path (i.e. TBD).

### Test locally

If you're updating in your local repo, you can test the changes in your machine by running `jekyll serve` in the command line (you can install jekyll by running `gem install bundler jekyll`). Then open localhost:4000 in the browser to see the changes.

## How the project is structured

The [help](link) folder will contain the following main folders:
- *_layouts*: it will have at the moment only one file ([default.html](link)) which will render the common HTML elements and the assets on every page.
- *_includes*: it will contain HTML content that can be reused on different pages. It will have only the file [main-navigation-tree.html](link) that will be rendered in the LHN.
- *assets*: it will contain the three sub-folders css, images, and js. The css folder can contain either .css or .sass files (where SASS files will be processed by Jekyll and it will generate a CSS file with the same file name in the same assets output folder).
- *send-money*: it will contain an index.html file with the content of the hub and will have a folder for each stage. Each sub-folder stage will contain all the articles related to that stage or subsection.
- *request-money*: same as send-money folder.
- *_site*: this will be the output folder that will contain the generated HTML files. This is the default name by Jekyll but can be changed.
index.html: it will have the homepage content.

More details about the Jekyll project structure can be found [here](https://jekyllrb.com/docs/structure/).
