CREATE SCHEMA IF NOT EXISTS AI_DEMOS.MASTER_DATA_MAPPING_DEMO;
USE SCHEMA AI_DEMOS.MASTER_DATA_MAPPING_DEMO;

/* Master Data */
CREATE OR REPLACE TABLE SOFTWARE_APPLICATIONS_MASTER (
    SOFTWARE_NAME VARCHAR(255)
);

INSERT INTO SOFTWARE_APPLICATIONS_MASTER (SOFTWARE_NAME) VALUES
('SAP'),
('Microsoft Teams'),
('Zoom'),
('Slack'),
('Salesforce'),
('Oracle Database'),
('ServiceNow'),
('Workday'),
('Adobe Creative Cloud'),
('Atlassian Jira'),
('Confluence'),
('GitHub'),
('GitLab'),
('Jenkins'),
('Docker'),
('Kubernetes'),
('AWS'),
('Microsoft Azure'),
('Google Cloud Platform'),
('Snowflake'),
('Tableau'),
('Power BI'),
('Looker'),
('Databricks'),
('Splunk'),
('Datadog'),
('New Relic'),
('PagerDuty'),
('Okta'),
('Duo Security'),
('CrowdStrike'),
('Palo Alto Networks'),
('Zscaler'),
('Fortinet'),
('Cisco Webex'),
('Microsoft Outlook'),
('Google Workspace'),
('Dropbox Business'),
('Box'),
('OneDrive'),
('SharePoint'),
('Trello'),
('Asana'),
('Monday.com'),
('Notion'),
('Airtable'),
('HubSpot'),
('Marketo'),
('Mailchimp'),
('Zendesk'),
('Freshdesk'),
('Intercom'),
('Twilio'),
('SendGrid'),
('Stripe'),
('PayPal'),
('Square'),
('QuickBooks'),
('NetSuite'),
('Xero'),
('Concur'),
('Expensify'),
('Coupa'),
('Ariba'),
('DocuSign'),
('Adobe Sign'),
('PandaDoc'),
('HelloSign'),
('Miro'),
('Figma'),
('Sketch'),
('InVision'),
('Canva'),
('Photoshop'),
('Illustrator'),
('Premiere Pro'),
('Final Cut Pro'),
('AutoCAD'),
('SolidWorks'),
('MATLAB'),
('Anaconda'),
('Visual Studio Code'),
('IntelliJ IDEA'),
('Eclipse'),
('PyCharm'),
('Postman'),
('Insomnia'),
('Terraform'),
('Ansible'),
('Puppet'),
('Chef'),
('Nagios'),
('Prometheus'),
('Grafana'),
('Elasticsearch'),
('MongoDB'),
('Redis'),
('PostgreSQL'),
('MySQL'),
('Microsoft SQL Server');

/* User-provided Data */
CREATE OR REPLACE TABLE SOFTWARE_APPLICATIONS_USER_PROVIDED (
    SOFTWARE_NAME VARCHAR(255)
);

INSERT INTO SOFTWARE_APPLICATIONS_USER_PROVIDED (SOFTWARE_NAME) VALUES
('sap'),                          -- SAP (lowercase)
('MS Teams'),                     -- Microsoft Teams (abbreviation)
('ZOOM'),                         -- Zoom (uppercase)
('slack'),                        -- Slack (lowercase)
('Sales Force'),                  -- Salesforce (space added)
('Oracle DB'),                    -- Oracle Database (abbreviation)
('Service Now'),                  -- ServiceNow (space added)
('Work Day'),                     -- Workday (space added)
('Adobe CC'),                     -- Adobe Creative Cloud (abbreviation)
('JIRA'),                         -- Atlassian Jira (uppercase, no vendor)
('git hub'),                      -- GitHub (lowercase, space)
('AWS Amazon'),                   -- AWS (expanded)
('Azure'),                        -- Microsoft Azure (shortened)
('GCP'),                          -- Google Cloud Platform (abbreviation)
('snow flake'),                   -- Snowflake (space, lowercase)
('TABLEAU'),                      -- Tableau (uppercase)
('PowerBI'),                      -- Power BI (no space)
('Cisco WebEx'),                  -- Cisco Webex (different casing)
('MS Outlook'),                   -- Microsoft Outlook (abbreviation)
('quick books'),                  -- QuickBooks (lowercase, space)
('Linear'),                       -- Not in Master
('Basecamp'),                     -- Not in Master
('Clockify');                     -- Not in Master

-- Create Search Service for Master Data
CREATE OR REPLACE CORTEX SEARCH SERVICE SOFTWARE_APPS_SEARCH
    ON SOFTWARE_NAME
    WAREHOUSE = AI_WH
    TARGET_LAG = '1 hour'
AS (
    SELECT SOFTWARE_NAME
    FROM SOFTWARE_APPLICATIONS_MASTER
);