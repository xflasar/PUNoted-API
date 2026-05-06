FROM alpine:3.19

# Install postgres tools, bash, gzip, and rclone (available in Alpine community repo)
RUN apk add --no-cache postgresql-client bash gzip rclone

WORKDIR /scripts

COPY backup.sh snapshot.sh ./
RUN chmod +x backup.sh snapshot.sh

# Schedule (Same as before)
RUN echo "0 0 * * * /scripts/backup.sh >> /var/log/cron.log 2>&1" > /etc/crontabs/root \
 && echo "5 0 * * * /scripts/snapshot.sh >> /var/log/cron.log 2>&1" >> /etc/crontabs/root

RUN mkdir -p /backups /var/log /config && touch /var/log/cron.log

CMD ["crond", "-f", "-d", "8"]