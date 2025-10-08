import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(sender_email, app_password, to_addrs, subject, body):
    """
    Sends an email using Gmail's SMTP server.

    Args:
        sender_email (str): The sender's full Gmail address.
        app_password (str): The 16-digit Google App Password.
        to_addrs (str, list): The recipient(s) email address(es). Can be a single string or a comma-separated string.
        subject (str): The subject of the email.
        body (str): The body of the email.
    """
    # Set up the SMTP server details for Gmail
    smtp_server = "smtp.gmail.com"
    port = 587

    # Convert to_addrs to proper format for SMTP
    if isinstance(to_addrs, str) and "," in to_addrs:
        # Comma-separated string, convert to list
        to_smtp = [addr.strip() for addr in to_addrs.split(",")]
        to_header = to_addrs  # Keep as comma-separated for header
    else:
        # Single recipient or already a list
        to_smtp = [to_addrs] if isinstance(to_addrs, str) else to_addrs
        to_header = to_addrs if isinstance(to_addrs, str) else ", ".join(to_addrs)

    # Create the email message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = to_header
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    try:
        # Create a secure SSL/TLS connection to the server
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()

        # Log in to the server
        server.login(sender_email, app_password)

        # Send the email
        server.sendmail(sender_email, to_smtp, message.as_string())

        print("Email sent successfully via Gmail SMTP!")

    except Exception as e:
        print(f"An error occurred: {e}")
        print(
            "Please check your email, ensure you are using a valid Google App Password, and that you have 2-Step Verification enabled."
        )
    finally:
        if "server" in locals() and server:
            server.quit()
