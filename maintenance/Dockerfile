FROM alpine:3.19

# Install postgres tools, bash, gzip, and rclone
RUN apk add --no-cache postgresql-client bash gzip rclone

WORKDIR /scripts

COPY backup.sh snapshot.sh ./
RUN chmod +x backup.sh snapshot.sh

# Schedule: Snapshot at 00:00, followed unconditionally by the DB Backup
RUN echo "0 0 * * * /scripts/snapshot.sh >> /var/log/cron.log 2>&1 ; /scripts/backup.sh >> /var/log/cron.log 2>&1" > /etc/crontabs/root

RUN mkdir -p /backups /var/log /config && touch /var/log/cron.log

CMD ["crond", "-f", "-d", "8"]