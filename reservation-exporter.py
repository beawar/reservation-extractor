import configparser
import pandas as pd
import mysql.connector
import smtplib
import ssl
# For guessing MIME type based on file name extension
import mimetypes

from datetime import datetime
# Here are the email package modules we'll need
from email.message import EmailMessage
from email.policy import SMTP


def extract_data(filepath, config):
    with mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['pass'],
        database=config['db']
    ) as mydb:

        sql = '''SELECT calendar_id, check_in, start_hour, end_hour
        FROM c8fzf_dopbsp_reservations
        WHERE check_in >= current_date() order by check_in, calendar_id, start_hour, end_hour'''

        result = pd.read_sql(sql, mydb)
        result.to_excel(filepath, index=False)


def send_mail(filepath, config, debug):
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['pass']
    filename = filepath.split('/')[-1]

    if host and port:
        subject = 'Reservations update'
        body = ('See attached file with updated reservations.\n'
                'Plase do not answer this mail, since it is generated from an automated system.\n'
                'For information regarding this mail and its content, please write to help@email.com')

        message = EmailMessage()
        message['From'] = config['from']
        message['To'] = config['to']
        message['CC'] = config['cc']
        message['BCC'] = config['bcc']
        message['Subject'] = subject
        # Add body to email
        message.set_content(body)

        # Guess the content type based on the file's extension.  Encoding
        # will be ignored, although we should check for simple things like
        # gzip'd or compressed files.
        ctype, encoding = mimetypes.guess_type(filename)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        with open(filepath, 'rb') as fp:
            message.add_attachment(fp.read(),
                                   maintype=maintype,
                                   subtype=subtype,
                                   filename=filename)

        # Set the filepath parameter
        message.add_header('Content-Disposition',
                           'attachment', filename=filename)

        if debug:
            fp.write(message.as_bytes(policy=SMTP))
        else:
            # Log in to server using secure context and send email
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context) as server:
                # server.set_debuglevel(1)
                server.login(user, password)
                server.send_message(message)
                server.quit()


if __name__ == '__main__':
    now = datetime.now()
    filepath = ('generated/reservation_from_' +
        now.strftime('%Y%m%d-%H%M%S') + '.xlsx')
    config = configparser.ConfigParser()
    config.read('reservation-exporter.ini')
    extract_data(filepath, config['mysqlDB'])
    send_mail(filepath, config['smtp'], False)
