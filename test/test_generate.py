####################################
##### Tests for otter generate #####
####################################

import os
import unittest
import subprocess
import json

from subprocess import PIPE
from glob import glob
from unittest import mock

# read in argument parser
bin_globals = {}

with open("bin/otter") as f:
    exec(f.read(), bin_globals)

parser = bin_globals["parser"]

TEST_FILES_PATH = "test/test-generate/"

class TestGenerate(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        create_image_cmd = ["make", "docker-test"]
        create_image = subprocess.run(create_image_cmd, stdout=PIPE, stderr=PIPE)
        assert not create_image.stderr, create_image.stderr.decode("utf-8")


    def test_docker(self):
        """
        Check that we have the right container installed and that docker is running
        """
        # use docker image inspect to see that the image is installed and tagged as otter-grader
        inspect = subprocess.run(["docker", "image", "inspect", "otter-test"], stdout=PIPE, stderr=PIPE)

        # assert that it didn't fail, it will fail if it is not installed
        self.assertEqual(len(inspect.stderr), 0, inspect.stderr.decode("utf-8"))
   

    def test_gs_generator(self):
        """
        Check that the correct zipfile is created by gs_generator.py
        """
        # create the zipfile
        generate_command = ["generate", "autograder",
            "-t", TEST_FILES_PATH + "tests",
            "-o", TEST_FILES_PATH,
            "-r", TEST_FILES_PATH + "requirements.txt",
            TEST_FILES_PATH + "test-df.csv"
        ]
        args = parser.parse_args(generate_command)
        args.func(args)

        # unzip the zipfile
        unzip_command = ["unzip", "-o", TEST_FILES_PATH + "autograder.zip", "-d", TEST_FILES_PATH + "autograder"]
        unzip = subprocess.run(unzip_command, stdout=PIPE, stderr=PIPE)
        self.assertEqual(len(unzip.stderr), 0, unzip.stderr)

        # go through files and ensure that they are correct
        for file in glob(TEST_FILES_PATH + "autograder/*"):
            if os.path.isfile(file):
                correct_file_path = os.path.join(TEST_FILES_PATH + "autograder-correct", os.path.split(file)[1])
                with open(file) as f:
                    with open(correct_file_path) as g:
                        self.assertEqual(f.read(), g.read(), "{} does not match {}".format(file, correct_file_path))
            else:
                for subfile in glob(os.path.join(file, "*")):
                    correct_file_path = os.path.join(TEST_FILES_PATH + "autograder-correct", os.path.split(file)[1], os.path.split(subfile)[1])
                    with open(subfile) as f:
                        with open(correct_file_path) as g:
                            self.assertEqual(f.read(), g.read(), "{} does not match {}".format(subfile, correct_file_path))

        # cleanup files
        cleanup_command = ["rm", "-rf", TEST_FILES_PATH + "autograder", TEST_FILES_PATH + "autograder.zip"]
        cleanup = subprocess.run(cleanup_command, stdout=PIPE, stderr=PIPE)
        self.assertEqual(len(cleanup.stderr), 0, cleanup.stderr.decode("utf-8"))


    def test_gradescope(self):
        """
        Checks that the Gradescope autograder works
        """
        # generate the zipfile
        generate_command = ["generate", "autograder",
            "-t", TEST_FILES_PATH + "tests",
            "-o", TEST_FILES_PATH,
            "-r", TEST_FILES_PATH + "requirements.txt",
            TEST_FILES_PATH + "test-df.csv"
        ]
        args = parser.parse_args(generate_command)
        args.func(args)

        # build the docker image
        build = subprocess.run(["docker", "build", TEST_FILES_PATH, "-t", "otter-gradescope-test"], stdout=PIPE, stderr=PIPE)
        self.assertEqual(len(build.stderr), 0, build.stderr.decode("utf-8"))

        # launch the container and return its container ID
        launch = subprocess.run(["docker", "run", "-dt", "otter-gradescope-test", "/autograder/run_autograder"], stdout=PIPE, stderr=PIPE)
        self.assertEqual(len(launch.stderr), 0, launch.stderr.decode("utf-8"))

        # get container ID
        container_id = launch.stdout.decode("utf-8")[:-1]

        # attach to the container and wait for it to finish
        attach = subprocess.run(["docker", "attach", container_id], stdout=PIPE, stderr=PIPE)
        self.assertEqual(len(attach.stderr), 0, attach.stderr.decode("utf-8"))
        
        # copy out the results.json file
        copy = subprocess.run(["docker", "cp", "{}:/autograder/results/results.json".format(container_id), "test/results.json"], stdout=PIPE, stderr=PIPE)
        self.assertEqual(len(copy.stderr), 0, copy.stderr.decode("utf-8"))

        # check that we got the right results
        with open(TEST_FILES_PATH + "results-correct.json") as f:
            correct_results = json.load(f)
        with open("test/results.json") as f:
            results = json.load(f)
        self.assertEqual(results, correct_results, "Incorrect results when grading in Gradescope container")

        # cleanup files and container
        cleanup = subprocess.run(["rm", "-rf", TEST_FILES_PATH + "autograder", TEST_FILES_PATH + "autograder.zip", "test/results.json"], stdout=PIPE, stderr=PIPE)
        remove_container = subprocess.run(["docker", "rm", container_id], stdout=PIPE, stderr=PIPE)
        self.assertEqual(len(cleanup.stderr), 0, cleanup.stderr.decode("utf-8"))
        self.assertEqual(len(remove_container.stderr), 0, remove_container.stderr.decode("utf-8"))