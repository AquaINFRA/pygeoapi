import logging
import subprocess
import json
import os
import sys
import argparse

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class TestProcessR(BaseProcessor):

    def __init__(self, processor_def):

        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.my_job_id = 'nothing-yet'

    def set_job_id(self, job_id: str):
        self.my_job_id = job_id

    def execute(self, data, outputs=None):

        DOWNLOAD_DIR = './'
        OWN_URL = 'https://aqua.igb-berlin.de/download/'
        R_SCRIPT_DIR = './pygeoapi/process'

        text1 = data.get('text1')
        text2 = data.get('text2')
        text3 = data.get('text3')

        downloadfilename = 'astra-%s.csv' % self.my_job_id
        downloadfilepath = DOWNLOAD_DIR.rstrip('/')+os.sep+downloadfilename
        # TODO: Carefully consider permissions of that directory!

        # Call R script, result gets stored to downloadfilepath
        R_SCRIPT_NAME = 'test_process_r.R'
        r_args = [text1, text2, text3, downloadfilepath]

        # Scripts are loaded via relative Path from os.getcwd()
        print(f"Loading R_SCRIPT from: {os.getcwd()}{R_SCRIPT_DIR[1:]}{os.sep}{R_SCRIPT_NAME}" )

        LOGGER.error('RUN R SCRIPT AND STORE TO %s!!!' % downloadfilepath)
        LOGGER.error('R ARGS %s' % r_args)
        exit_code, err_msg = call_r_script('1', LOGGER, R_SCRIPT_NAME, R_SCRIPT_DIR, r_args)
        LOGGER.error('RUN R SCRIPT DONE: CODE %s, MSG %s' % (exit_code, err_msg))

        if not exit_code == 0:
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(user_msg=f"R script failed with exit code {exit_code}. Error Message:"
                                                 f" {err_msg}")
        else:
            LOGGER.error('CODE 0 SUCCESS!')

            # Create download link:
            downloadlink = OWN_URL.rstrip('/')+os.sep+downloadfilename
            # TODO: Again, carefully consider permissions of that directory!

            # Return link to file:
            response_object = {
                "outputs": {
                    "first_result": {
                        "title": "Astras and Natalijas First Result",
                        "description": "must ask astra what this is",
                        "href": downloadlink
                    }
                }
            }

            return 'application/json', response_object

    def __repr__(self):
        return f'<TestProcessR> {self.name}'


def call_r_script(num, LOGGER, r_file_name, path_rscripts, r_args):

    LOGGER.debug('Now calling bash which calls R: %s' % r_file_name)
    r_file = path_rscripts.rstrip('/')+os.sep+r_file_name
    cmd = ["/usr/bin/Rscript", "--vanilla", r_file] + r_args
    LOGGER.info(r_args)
    LOGGER.info(cmd)
    LOGGER.debug('Running command... (Output will be shown once finished)')
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate()
    LOGGER.debug("Done running command! Exit code from bash: %s" % p.returncode)

    ### Print stdout and stderr
    stdouttext = stdoutdata.decode()
    stderrtext = stderrdata.decode()
    if len(stderrdata) > 0:
        err_and_out = 'R stdout and stderr:\n___PROCESS OUTPUT {n}___\n___stdout___\n{stdout}\n___stderr___\n{stderr}   (END PROCESS OUTPUT {n})\n___________'.format(
            stdout= stdouttext, stderr=stderrtext, n=num)
        LOGGER.error(err_and_out)
    else:
        err_and_out = 'R stdour:\n___PROCESS OUTPUT {n}___\n___stdout___\n{stdout}\n___stderr___\n___(Nothing written to stderr)___\n   (END PROCESS OUTPUT {n})\n___________'.format(
            stdout = stdouttext, n = num)
        LOGGER.info(err_and_out)
    return p.returncode, err_and_out


""" if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Define a processor definition with the necessary keys
    processor_def = {
        'name': 'TestProcessR',
        'title': 'Test Process R',
        'description': 'A test process that calls an R script',
        'type': 'process'
    }

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run TestProcessR with specified text inputs.')
    parser.add_argument('--text1', type=str, required=True, help='First text input')
    parser.add_argument('--text2', type=str, required=True, help='Second text input')
    parser.add_argument('--text3', type=str, required=True, help='Third text input')
    args = parser.parse_args()

    # Create an instance of your processor
    processor = TestProcessR(processor_def)
    
    # Set a job id
    processor.set_job_id('example-job-id')

    # Define the data from command line arguments
    sample_data = {
        'text1': args.text1,
        'text2': args.text2,
        'text3': args.text3
    }

    # Execute the process
    try:
        content_type, response = processor.execute(sample_data)
        # Print the result
        print(f'Content Type: {content_type}')
        print(f'Smple: {sample_data}')
        print(f'Response: {json.dumps(response, indent=2)}')
    except ProcessorExecuteError as e:
        print(f'Error: {e}') """