#! /bin/sh

# 변수 선언
REMOTE_FTP_ADDR="172.30.1.222"
PORT="22"
REMOTE_USER="card01"
REMOTE_PASSWORD="card0123!"
REMOTE_UPLOAD_PATH="/app/card01/cardsales/bin"
FILE_NAME="cardsales"

# expect 를 사용하여 대화식 명령 실행
expect << EOF
spawn sftp -oPort=$PORT $REMOTE_USER@$REMOTE_FTP_ADDR
expect {
    "password:" {
        send "${REMOTE_PASSWORD}\r"
    }
    "connecting (yes/no)?" {
        send "yes\r"
        exp_continue
    }
}
expect "sftp>" { send "cd $REMOTE_UPLOAD_PATH\r" }
expect "sftp>" { send "put $FILE_NAME\r" }
expect "sftp>" { send "bye\r" }
expect eof
EOF
