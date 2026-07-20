#!/bin/bash
# Step 1: Run your data update
python3 /home/soumyamitra2010/Momentum_Calculator/app.py

# Step 2: Reload the web app
touch /var/www/soumyamitra2010_pythonanywhere_com_wsgi.py

echo "Data updated and Web App reloaded!"