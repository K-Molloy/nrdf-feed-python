# NRDF Feed (Python)

NRDF (Network Rail Data Feeds) Feed is the [STOMP][stomp] powered listener system for [Kieran Molloy's][personal-website] 3rd year university dissertation.

The system was previously created in JavaScript using [node.js] and can be found [here][old-github-repo].
Currently the plan is to transition over to python for better performance, in addition to the far fewer dependencies and easier updatability this decision is for the long run.
When TRUST and TD is implemented the website will be recreated, spliting the current (very naively created) API which is integrated directly into the webserver's (again using [node.js] with [express]) viewmodel API.

# Road Map

  - [x] SMART Integration - 
  - [x] CORPUS Integration - 
  - [x] Reference Integration - 
  - [x] Full Schedule Integration - 
  - [x] Update Schedule Integration - 
  - [-] TRUST Tracking - All Movements integrated into a single record
  - [-] TRUST API - Similar to the train models from the [previous website][site-github-repo]
  - [-] TD Tracking - All Berth Movements saved in a 'berth-map' datatype
  - [-] TD API - Request single berth's current occupany / history in addition to regions
  - [-] Fancy Website - TBC, but an easy way to access the API focusing on usability
  - [-] VSTP Integration - TBC
  - [-] RTPPM Integration - TBC
  - [-] TSR Integration -TBC
  - [-] New CIF Parser - TBC

### Installation

NRDF Feed requires [Python][python] 3.7+ to run.

Create a [virtual environment][venv] then install the dependencies, modify the configuration and start it up.

```sh
$ cd nrdf-feed-python
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ mv example.config.json config.json
$ python3 networkrail-simple.py
```
The testing database configuration is in 'mongo-test', for live environments 'mongo-live'.

For production environments, specify in the 'mongo-type',

TODO: production installation

### Dependencies

Dillinger is currently extended with the following plugins. Instructions on how to use them in your own application are linked below.

| Package | Version | Plan |
| ------ | ------ | ------ |
| [numpy] | 1.18.5 | Essential |
| [pymongo] | 3.10.1 | Essential |
| [stomp] | 6.0.0 | Essential |
| [requests] | 2.23.0 | To be removed after installation stage |
| [pandas] | 1.0.4 | To be removed in future versions |


### Development

Want to contribute? Great!

NRDF-Feed is currently in a really early phase and is rapidly developing
Feel free to branch or create a new issue!

### Docker
NRDF Feed is being prepared for a docker realease for convenient deplyonemnt.



License
----

MIT


**Free Software, Hell Yeah!**

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)

   [python]: <https://www.python.org/downloads/release/python-383/>
   [pip]: <https://pypi.org/project/pip/>
   [stomp]: <https://github.com/jasonrbriggs/stomp.py>
   [pandas]: <https://pandas.pydata.org/>
   [numpy]: <https://numpy.org/>
   [requests]: <https://pypi.org/project/requests/>
   [venv]: <https://docs.python.org/3/tutorial/venv.html>
   [pymongo]: <https://pypi.org/project/pymongo/>
   [mongodb]: <https://docs.mongodb.com/manual/>
   [this-github-repo]: <https://github.com/K-Molloy/nrdf-feed-python>
   [old-github-repo]: <https://github.com/K-Molloy/nrdf-feed>
   [site-github-repo]: <https://github.com/K-Molloy/nrdf-site>
   [personal-website]: <https://kieranmolloy.co.uk/>
   [nrdf-website]: <https://nrdf.kieranmolloy.co.uk/>
   
