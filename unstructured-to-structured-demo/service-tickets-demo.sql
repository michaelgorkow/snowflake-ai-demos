USE DATABASE AI_DEMOS;
CREATE SCHEMA IF NOT EXISTS AI_DEMOS.AI_FUNCTIONS_DEMO;
USE SCHEMA AI_DEMOS.AI_FUNCTIONS_DEMO;

-- Create Data
CREATE OR REPLACE TABLE SERVICE_TICKETS (
    TICKET_DESCRIPTION VARCHAR(5000)
);

INSERT INTO SERVICE_TICKETS VALUES
('Error message E-4102 and E-5301 displayed during startup of SOMATOM Force. Critical system failure with gantry not starting correctly and tube voltage issues. Cooling system running but detector calibration failing. Technician John Smith on site: Defective high voltage module identified and replaced. System calibration performed.'),

('Wartung am SOMATOM Drive erfolgreich abgeschlossen. Alle Systeme funktionieren nach Routineprüfung einwandfrei. Tischbewegung gleichmäßig und präzise. Kalibrierungstests zeigen optimale Leistung. Technikerin Sarah Johnson: Patientendurchsatz nach Anpassungen deutlich verbessert.'),

('Regelmäßige Wartung am SOMATOM Pro.Pulse durchgeführt. System funktioniert wie erwartet. Techniker Mike Brown: Detektormodul in Sektor 3 überprüft und arbeitet innerhalb normaler Parameter. Standardkalibrierung abgeschlossen. Bildqualität entspricht den Spezifikationen.'),

('Outstanding performance improvement achieved on SOMATOM Drive workstation. Technician David Lee: 3D reconstruction speed increased by 50%. New cooling system working excellently. Graphics processing now runs smoothly with zero crashes. Patients and staff extremely satisfied with the enhanced capabilities.');

SELECT * FROM SERVICE_TICKETS;

-- Unstructured -> Structured: Automated Pipeline with Dynamic Tables
CREATE OR REPLACE DYNAMIC TABLE STRUCTURED_SERVICE_TICKETS
  TARGET_LAG = '20 minutes'
  WAREHOUSE = AI_WH
  AS
    SELECT 
      *,
      -- General Purpose LLM
      AI_COMPLETE(
        'claude-sonnet-4-6', 
        'What is the name of the service technician according to the ticket? Only return the name. The ticket:' || TICKET_DESCRIPTION)::TEXT AS SERVICE_TECHNICIAN,
      
      -- Information Extraction
      AI_EXTRACT(
        text => TICKET_DESCRIPTION,
        responseFormat => {
          'schema': {
            'type': 'object',
            'properties': {
              'error_codes': {
                'description': 'What error codes are mentioned in the ticket?',
                'type': 'array'
              }
            }
          }
        }
      )['response']['error_codes']::ARRAY AS ERROR_CODES,
      
      -- Classification
      AI_CLASSIFY(TICKET_DESCRIPTION, ['SOMATOM Force', 'SOMATOM Drive', 'SOMATOM Pro.Pulse'])['labels'][0]::TEXT AS MACHINE_TYPE,
      
      -- Sentiment Detection
      AI_SENTIMENT(TICKET_DESCRIPTION)['categories'][0]['sentiment']::TEXT AS SENTIMENT,
      
      -- Translation
      AI_TRANSLATE(TICKET_DESCRIPTION, '', 'en') AS TRANSLATION_EN
    
    FROM
      SERVICE_TICKETS;

SELECT * FROM STRUCTURED_SERVICE_TICKETS;