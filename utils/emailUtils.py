import boto3
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template

# Load configuration
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

def send_email(to_email, subject, template_path, template_vars):
    ses_client = boto3.client(
        'ses',
        region_name=config['AWS_REGION'],
        aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
    )
    try:
        # Read the email template
        with open(template_path, 'r') as template_file:
            template_content = template_file.read()
        # Create a Jinja2 template
        template = Template(template_content)
        # Render the template with the provided variables
        email_body = template.render(template_vars)
        # Create the email message
        msg = MIMEMultipart()
        msg['From'] = config['EMAIL_HOST']
        msg['To'] = to_email
        msg['Subject'] = subject
        # Attach the email body to the message
        msg.attach(MIMEText(email_body, 'html'))
        # Send the email using AWS SES
        response = ses_client.send_raw_email(
            Source=config['EMAIL_HOST'],
            Destinations=[to_email],
            RawMessage={
                'Data': msg.as_string()
            }
        )
        print(f"Email sent successfully: {response['MessageId']}")
        return "Email sent successfully"
    except Exception as e:
        print(f"Error: {e}")
        return "Email could not be sent"
