LOCAL_VARIABLES := FILESAVE_DIRECTORY="~/" \
	CRICSHEET_IPL_FILES_DIRECTORY="~/Downloads/ipl_json/" \
	CRICSHEET_T20I_FILES_DIRECTORY="~/Downloads/t20is_json/" \
	CORE_PLATFORM_BASEURL="http://10.161.24.199:30046/core-platform" \
	BABEL_SERVICE_BASEURL="http://10.161.24.199:30046/babel-service" \
	CHOWKIDAAR_PLATFORM_BASEURL="http://10.161.24.199:30046/chowkidaar-service" \
	CHOWKIDAAR_GRANT_TYPE="client_credentials" \
    CHOWKIDAAR_CLIENT_ID="core-platform-internal" \
    CHOWKIDAAR_CLIENT_SECRET="7a6f045f-824d-40ef-9a58-1ca03ee306e1" \

create_env:
	virtualenv venv
	echo "now call: source ./venv/bin/activate"

activate_env:
	source ./venv/bin/activate

install:
	pip3 install -r ./requirements.txt

test:
	echo "testing"
	$(LOCAL_VARIABLES) pytest

run:
	$(LOCAL_VARIABLES) uvicorn --host 0.0.0.0 --port 4200 src.server:app --reload

package:
	echo "to create a docker build/ python package/code artefacts"
	docker-compose -f ./docker-compose.yml build

publish:
	echo "to publish a created docker image / code artefacts to a cental repository"
	docker-compose -f ./docker-compose.yml push

run_package:
	docker-compose -f docker-compose.yml up --remove-orphans

deploy:
	echo "deploying"

lint:
	black --check .

test-ci:
	echo "testing"
	$(LOCAL_VARIABLES) pytest --cov=src --cov-report=html --no-coverage-upload
	echo "Running CSS JS Hack utility since Microsoft did an awesome job on CSP security settings."
	css_js_inliner