require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'

# Initialize the logger
logger = Logger.new(STDOUT)

# Define the URL of the page
url = 'https://www.warwyn.tas.gov.au/planning-and-development/advertised-permits/'

# Step 1: Fetch the page content
begin
  logger.info("Fetching page content from: #{url}")
  page_html = open(url).read
  logger.info("Successfully fetched page content.")
rescue => e
  logger.error("Failed to fetch page content: #{e}")
  exit
end

# Step 2: Parse the page content using Nokogiri
doc = Nokogiri::HTML(page_html)

# Step 3: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

# Create table
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS waratah_wynyard (
    id INTEGER PRIMARY KEY,
    description TEXT,
    date_scraped TEXT,
    date_received TEXT,
    on_notice_to TEXT,
    address TEXT,
    council_reference TEXT,
    applicant TEXT,
    owner TEXT,
    stage_description TEXT,
    stage_status TEXT,
    document_description TEXT,
    title_reference TEXT
  );
SQL

# Define variables for storing extracted data for each entry
address = ''  
description = ''
on_notice_to = ''
title_reference = ''
date_received = ''
council_reference = ''
applicant = ''
owner = ''
stage_description = ''
stage_status = ''
document_description = ''
date_scraped = Date.today.to_s

doc.css('.wpfd-search-result').each_with_index do |row, index|
# Extract the title from the <a> tag's title attribute
title_reference = row.at_css('.wpfd_downloadlink')['title']

# Extract council reference (DA number from the title)
council_reference = title_reference.split(' - ').first

# Extract address from the title (using regex to capture the address part)
address = title_reference.match(/(\d+[A-Za-z]*\s[\w\s,]+)/)&.captures&.first

# Extract description from the title (everything between the address and "Notification expiry date")
description = title_reference.match(/-\s([^-\d]+)-\sNotification expiry date/)&.captures&.first&.strip

# Extract the on_notice_to date from the title
on_notice_to = title_reference.match(/(\d{1,2} [A-Za-z]+ \d{4})/)&.captures&.first

# Document URL (from the <a> tag in the 'Download' column)
document_description = row.at_css('.wpfd_downloadlink')['href']

# Log the extracted data for debugging purposes
logger.info("Extracted Data: Title: #{description}, Address: #{address}, Council Reference: #{council_reference}, On Notice To: #{on_notice_to}, Document URL: #{document_description}")

  
  # Step 5: Ensure the entry does not already exist before inserting
  existing_entry = db.execute("SELECT * FROM waratah_wynyard WHERE council_reference = ?", council_reference)

  if existing_entry.empty?  # Only insert if the entry doesn't already exist
    # Save data to the database
    db.execute("INSERT INTO waratah_wynyard 
      (description, date_scraped, date_received, on_notice_to, council_reference, document_description, title_reference) 
      VALUES (?, ?, ?, ?, ?, ?, ?)",
      [description, date_scraped, date_received, on_notice_to, council_reference, document_description, title_reference])

    logger.info("Data for #{council_reference} saved to database.")
  else
    logger.info("Duplicate entry for document #{council_reference} found. Skipping insertion.")
  end
end

# Finish
logger.info("Data has been successfully inserted into the database.")
