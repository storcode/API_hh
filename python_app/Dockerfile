FROM python:3.10

RUN apt update && apt -y install cron
RUN pip install --upgrade pip
RUN pip install pika
RUN pip install requests
RUN pip install psycopg2-binary
RUN pip install DateTime

# Copy cron-docker file to the cron.d directory
COPY ./cron_docker /etc/cron.d/crontab
 
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/crontab

# Apply cron job
RUN /usr/bin/crontab /etc/cron.d/crontab
 
# Create the log file to be able to run tail
RUN touch /var/log/cron_docker.log
 
# Run the command on container startup
CMD ["cron", "-f"]