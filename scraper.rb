require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'
require 'cgi'

# Set up a logger to log the scraped data
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

# Create table if it doesn't already exist
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

logger.info("Start Extraction of Data")

# Loop through all rows in the table
doc.css('.wpfd-search-result').each_with_index do |row, index|
  # Extract the title (DA number + address)
  title_reference_element = row.at_css('.wpfd-file-crop-title')

  if title_reference_element
    title_reference = title_reference_element.text.strip

    # Extract the council reference (DA number)
    council_reference = title_reference.split(' - ').first.strip

    # Extract the address (the part between DA number and description)
    address = title_reference.match(/DA\d+ - (.*?)(?= - )/) ? title_reference.match(/DA\d+ - (.*?)(?= - )/)[1].strip : 'Address not found'

    # Extract the description (everything after the last " - " in the title)
    description = title_reference.split(' - ').last.strip

    # Extract the "date_received" (Date modified column)
    date_received = row.at_css('.file_modified') ? row.at_css('.file_modified').text.strip : 'Date not found'
    date_received = Date.parse(date_received).strftime('%Y-%m-%d') if date_received != 'Date not found'

    # Extract the PDF link (from the Download column)
    document_description = row.at_css('.wpfd_downloadlink')['href'] if row.at_css('.wpfd_downloadlink')

    # Calculate "on_notice_to" date as 14 days after the "date_received"
    on_notice_to = (Date.parse(date_received) + 14).strftime('%Y-%m-%d') if date_received != 'Date not found'

    # Log the extracted data for debugging purposes
    logger.info("Council Reference: #{council_reference}")
    logger.info("Address: #{address}")
    logger.info("Description: #{description}")
    logger.info("Date Received: #{date_received}")
    logger.info("On Notice To: #{on_notice_to}")
    logger.info("PDF Link: #{document_description}")
    logger.info("-----------------------------------")

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
  else
    logger.warn("No title found for row #{index}. Skipping row.")
  end
end

# Finish
logger.info("Data has been successfully inserted into the database.")
