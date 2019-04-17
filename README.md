# RedditDataScraper

1. Properties file template to be configured is located under main/controller folder. Copy and change name to "properties_local.ini".

2. Use this resource to determine the various configuration properties under "Credentials": http://www.storybench.org/how-to-scrape-reddit-with-python/.

3. The database has a default configuration of the default MongoDB server location and it will write to "Post" unless changed in the section "Database".

4. To run, fill out all credentials on "properties_local.ini" and navigate to the folder and run "Python3 RedditDataScraperController.py". It will automatically load the class and run at the bottom of the file.

