# LinkedIn Job Data Extractor

This project extracts job data from LinkedIn based on specified keywords and locations. It runs weekly using GitHub Actions and sends the results via email.

## Getting Started

### Prerequisites

- Python 3.11.7
- GitHub account
- RapidAPI account (for LinkedIn Data Scraper API)

### Forking/Cloning the Repository

1. Fork the repository by clicking the "Fork" button at the top right of the repository page.
2. Clone your forked repository:

``` bash
git clone https://github.com/Chideraozigbo/Linkedln-Jobs-Data-Pipeline.git
cd Linkedln-Jobs-Data-Pipeline
```
### Changing the Job Keyword

1. Open the `pipeline.py` file.
2. Locate the `extract_data` function.
3. Find the `payload` dictionary and modify the "keywords" value:

```python
payload = {
    "keywords": "Your Desired Job Title",
    "location": "California, United States",
    "count": 100
}
```
Note: You can choose any desired State and Country of your choice in the keyword.

4. Save the file.

### Setting Up Secrets for GitHub Actions

1. Go to your forked repository on GitHub.
2. Click on "Settings" > "Secrets and variables" > "Actions".
3. Click "New repository secret" and add the following secrets:
- Name: `LINKEDIN_API_CONFIG` Value: Your RapidAPI configuration in JSON

```json
{
  "X-RapidAPI-Key": "your-rapidapi-key",
  "x-rapidapi-host": "linkedin-data-scraper.p.rapidapi.com",
  "Content-Type": "application/json"
}
```

- Name: `EMAIL_USER`Value: Your Gmail address
- Name: `EMAIL_PASS`Value: Your Gmail app password (not your regular password)

### Customizing the Workflow

1. Open the `.github/workflows/linkedin_data_extraction.yml` file.
2. Modify the cron schedule if needed (currently set to run every Sunday at 10:00 AM).
3. Update the email recipient in the "Send email with CSV file" step if desired.

### Running the Workflow
The workflow will run automatically according to the schedule. To run it manually:

1. Go to the "Actions" tab in your repository.
2. Select the "Weekly LinkedIn Data Extraction" workflow.
3. Click "Run workflow" and then "Run workflow" again to confirm.

### Additional Information

- Make sure to keep your API key and email credentials secure.
- The extracted data will be saved in the file/ directory with a timestamp in the filename.
- Check the `logs/log.txt` file for execution logs.

### Troubleshooting
If you encounter any issues:

1. Check the Actions tab for any error messages in the workflow runs.
2. Ensure all secrets are correctly set up.
3. Verify your RapidAPI subscription and key are active and correct.

For more help, please open an issue in the repository.

