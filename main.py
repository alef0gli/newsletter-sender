import smtplib
import csv
import time
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from string import Template
import yaml
from datetime import datetime
from pathlib import Path
from smtplib import SMTPServerDisconnected, SMTPAuthenticationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('newsletter.log'),
        logging.StreamHandler()
    ]
)

class NewsletterSender:
    def __init__(self, config_path='config.yml'):
        print("Initializing NewsletterSender...")
        self.config = self._load_config(config_path)
        self.sent_count = 0
        self.last_send_time = 0
        print("Initialization complete.")
    
    def _load_config(self, config_path):
        """Load SMTP and sending configuration from YAML file"""
        print(f"Loading configuration from {config_path}...")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print("Configuration loaded successfully.")
        return config
    
    def _read_template(self, template_path):
        """Read HTML template file"""
        with open(template_path, 'r', encoding='utf-8') as f:
            return Template(f.read())
    
    def _read_recipients(self, csv_path):
        """Read recipient data from CSV file"""
        recipients = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                recipients.append(row)
        return recipients
    
    def _rate_limit(self):
        """Implement rate limiting to avoid spam filters"""
        if self.sent_count >= self.config['rate_limit']['emails_per_batch']:
            batch_delay = self.config['rate_limit']['batch_delay']
            print(f"\nBatch limit reached. Waiting {batch_delay} seconds...")
            
            for remaining in range(batch_delay, 0, -1):
                print(f"\rResuming in {remaining} seconds...  ", end='', flush=True)
                time.sleep(1)
            print("\rResuming now!                           ")
            
            self.sent_count = 0
        else:
            current_time = time.time()
            time_since_last = current_time - self.last_send_time
            if time_since_last < self.config['rate_limit']['delay_between_emails']:
                wait_time = self.config['rate_limit']['delay_between_emails'] - time_since_last
                print(f"\rWaiting {wait_time:.1f} seconds...", end='', flush=True)
                time.sleep(wait_time)
                print("\r                           ", end='', flush=True)
    
    def _test_smtp_connection(self):
        """Test SMTP connection before sending batch emails"""
        try:
            print("Attempting SSL connection to SMTP server...")
            with smtplib.SMTP_SSL(self.config['smtp']['host'], self.config['smtp']['port']) as server:
                server.login(self.config['smtp']['username'], self.config['smtp']['password'])
                logging.info("SMTP connection test successful")
                print("SMTP connection test successful!")
                return True
        except SMTPAuthenticationError:
            logging.error("SMTP authentication failed. Please check your credentials.")
            raise
        except Exception as e:
            logging.error(f"SMTP connection test failed: {str(e)}")
            raise
    
    def send_newsletters(self, template_path, csv_path):
        """Main method to send newsletters to all recipients"""
        print("\n=== Starting Newsletter Sending Process ===")
        print(f"Template: {template_path}")
        print(f"Recipients: {csv_path}")
        
        # Test SMTP connection first
        print("\nTesting SMTP connection...")
        self._test_smtp_connection()
        
        template = self._read_template(template_path)
        recipients = self._read_recipients(csv_path)
        total_recipients = len(recipients)
        print(f"\nFound {total_recipients} recipients to process")
        
        # Create results directory if it doesn't exist
        results_dir = Path('results')
        results_dir.mkdir(exist_ok=True)
        
        # Create results file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = results_dir / f'sending_results_{timestamp}.csv'
        print(f"Results will be saved to: {results_file}")
        
        with open(results_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['email', 'status', 'error_message'])
            
            try:
                print("\nConnecting to SMTP server...")
                with smtplib.SMTP_SSL(self.config['smtp']['host'], self.config['smtp']['port']) as server:
                    server.login(self.config['smtp']['username'], self.config['smtp']['password'])
                    print("Connected successfully!")
                    
                    for index, recipient in enumerate(recipients, 1):
                        print(f"\nProcessing {index}/{total_recipients}: {recipient['email']}")
                        retries = 3
                        for attempt in range(retries):
                            try:
                                if attempt > 0:
                                    print(f"Retry attempt {attempt + 1}/{retries}")
                                
                                self._rate_limit()
                                
                                # Create email
                                msg = MIMEMultipart('alternative')
                                msg['Subject'] = self.config['email']['subject']
                                msg['From'] = self.config['email']['from']
                                msg['To'] = recipient['email']
                                
                                # Add HTML content
                                with open(template_path, 'r', encoding='utf-8') as f:
                                    html = f.read()
                                msg.attach(MIMEText(html, 'html'))
                                
                                # Send email
                                print("Sending email...", end=' ', flush=True)
                                server.send_message(msg)
                                print("✓ Sent!")
                                
                                logging.info(f"Successfully sent email to {recipient['email']}")
                                writer.writerow([recipient['email'], 'success', ''])
                                
                                self.sent_count += 1
                                self.last_send_time = time.time()
                                break
                                
                            except SMTPServerDisconnected:
                                if attempt < retries - 1:
                                    print(f"Connection lost, retrying in 5 seconds...")
                                    time.sleep(5)
                                    print("Reconnecting to SMTP server...")
                                    # Create new SSL connection on reconnect
                                    server = smtplib.SMTP_SSL(self.config['smtp']['host'], self.config['smtp']['port'])
                                    server.login(self.config['smtp']['username'], self.config['smtp']['password'])
                                else:
                                    raise
                            except Exception as e:
                                error_msg = str(e)
                                print(f"❌ Error: {error_msg}")
                                logging.error(f"Failed to send to {recipient['email']}: {error_msg}")
                                writer.writerow([recipient['email'], 'failed', error_msg])
                                break
                    
                print("\n=== Newsletter Sending Process Complete ===")
                
            except Exception as e:
                print(f"\n❌ Fatal Error: {str(e)}")
                logging.error(f"SMTP connection error: {str(e)}")
                raise

def main():
    try:
        print("Starting newsletter sending script...")
        sender = NewsletterSender()
        sender.send_newsletters('template.html', 'recipients.csv')
        print("\nScript completed successfully!")
    except Exception as e:
        print(f"\nScript failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
