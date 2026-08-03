@echo off
cd "C:\Users\Rohit\OneDrive\Desktop\Claude_Project\Hika"
git pull --rebase origin main
git add -A
git commit -m "sync"
git push
echo Done - Railway is deploying
pause