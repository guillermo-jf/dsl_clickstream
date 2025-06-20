import base64
import os
import json
import logging

# Using SendGrid for sending email.
# You'll need to:
# 1. Create a SendGrid account and an API Key.
# 2. Add the SendGrid API Key as an environment variable `SENDGRID_API_KEY` for this Cloud Function.
# 3. Set the `NOTIFY_EMAIL` environment variable to the desired recipient's email address.
# 4. Optionally, set `SENDER_EMAIL` environment variable (verified sender in SendGrid).
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL") # This should be a verified sender in your SendGrid account


def send_notification_email(recipient_email, subject, body_html):
    """Sends an email using SendGrid."""
    if not SENDGRID_API_KEY:
        logging.error("SENDGRID_API_KEY not set. Cannot send email.")
        return False
    if not recipient_email:
        logging.error("Recipient email (NOTIFY_EMAIL) not set. Cannot send email.")
        return False
    if not SENDER_EMAIL:
        logging.error("SENDER_EMAIL not set. Cannot send email. Please set a verified sender email.")
        return False

    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=recipient_email,
        subject=subject,
        html_content=body_html
    )
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logging.info(f"Email sent to {recipient_email}, status code: {response.status_code}")
        return 200 <= response.status_code < 300
    except Exception as e:
        logging.error(f"Error sending email via SendGrid: {e}", exc_info=True)
        return False


def process_pubsub_notification(event, context):
    """
    Cloud Function triggered by a Pub/Sub message.
    Sends an email notification about a processed file.
    """
    if not NOTIFY_EMAIL:
        logging.error("NOTIFY_EMAIL environment variable not set. No notification will be sent.")
        return # Or raise an error if this is critical

    try:
        pubsub_message_data_str = base64.b64decode(event['data']).decode('utf-8')
        logging.info(f"Received Pub/Sub message data: {pubsub_message_data_str}")

        try:
            message_payload = json.loads(pubsub_message_data_str)
            file_name = message_payload.get("file_name", "FILE NOT PROVIDED")
            status = message_payload.get("status", "STATUS NOT PROVIDED")
            details = message_payload.get("details", "No additional details provided.")

            subject = f"Notification: File '{file_name}' Processed"
            body_html = f"""
            <p>Hello,</p>
            <p>This is an automated notification to inform you that the file <strong>{file_name}</strong> has been processed.</p>
            <p><strong>Status:</strong> {status}</p>
            <p><strong>Details:</strong></p>
            <pre>{json.dumps(details, indent=2) if isinstance(details, (dict, list)) else details}</pre>
            <p>Regards,<br/>Your Data Pipeline Notification System</p>
            """
        except json.JSONDecodeError:
            logging.warning("Pub/Sub message data was not valid JSON. Sending a generic notification.")
            subject = "Notification: File Processing Update"
            body_html = f"<p>Hello,</p><p>A file processing task has completed. Raw message data:</p><pre>{pubsub_message_data_str}</pre>"

        send_notification_email(NOTIFY_EMAIL, subject, body_html)

    except Exception as e:
        logging.error(f"Error processing Pub/Sub message or sending email: {e}", exc_info=True)
        # Depending on the error, you might want to raise it to allow Cloud Functions to retry.
        # For now, we log the error and let the function complete.