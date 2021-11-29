LOCAL_VARIABLES := FILESAVE_DIRECTORY="~/" \
	CRICSHEET_IPL_FILES_DIRECTORY="~/Downloads/ipl_json/" \
	CRICSHEET_T20I_FILES_DIRECTORY="~/Downloads/t20is_json/" \

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

cricsheet_ipl:
	$(LOCAL_VARIABLES) python ./data_extractors/cricsheet/ipl/matches_extractors.py
    python ./data_extractors/cricsheet/ipl/balls_extractors.py

cricsheet_t20is:
	$(LOCAL_VARIABLES) python ./data_extractors/cricsheet/t20is/matches_extractors.py
	python ./data_extractors/cricsheet/t20is/balls_extractors.py