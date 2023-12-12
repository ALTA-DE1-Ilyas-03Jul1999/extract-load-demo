if [ ! -d .venv ]
then
    python -m venv .venv
fi
source .venv/bin/activate
pip install -r requirements.txt

if [ -f .env ]
then
    source .env
else
    cp template.env .env
    echo "Please modify your .env and reload this script (i.e., source project.sh)"
fi