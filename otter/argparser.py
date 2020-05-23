############################################
##### Argument Parser for Otter Grader #####
############################################

import argparse

from . import assign
from . import check
from . import export
from . import generate
from . import grade
from . import service


def get_parser():
    """Creates and returns the argument parser for Otter"""

    parser = argparse.ArgumentParser(description="""
    A Python-based autograder for Jupyter Notebooks and Python scripts that runs locally on the instructors machine.
    Also supports use of Gradescope's autograding service, assignment distribution with otter-assign, and public tests
    that students can run while working on assignments.
    """)
    subparsers = parser.add_subparsers()


    ##### PARSER FOR otter assign #####
    assign_parser = subparsers.add_parser("assign", description="Create distribution versions of otter-assign-formatted notebook")
    assign_parser.add_argument("master", help="Notebook with solutions and tests.")
    assign_parser.add_argument("result", help="Directory containing the result.")
    assign_parser.add_argument("--no-export-cell", help="Don't inject an export cell into the notebook", default=False, action="store_true")
    assign_parser.add_argument("--no-run-tests", help="Don't run tests.", default=False, action="store_true")
    assign_parser.add_argument("--no-init-cell", help="Don't automatically generate an Otter init cell", default=False, action="store_true")
    assign_parser.add_argument("--no-check-all", help="Don't automatically add a check_all cell", default=False, action="store_true")
    assign_parser.add_argument("--no-filter", help="Don't filter the PDF.", default=False, action="store_true")
    assign_parser.add_argument("--instructions", help="Additional submission instructions for students")
    assign_parser.add_argument("--jassign", default=False, action="store_true", help="Use jassign notebook format")
    assign_parser.add_argument("--debug", default=False, action="store_true", help="Do not ignore errors in running tests for debugging")

    # generate options -- COMMENTED OUT BECAUSE YOU SHOULD USE THE ASSIGNMENT METADATA FOR THIS
    assign_parser.add_argument("--generate", default=False, action="store_true", help="Generate Gradescope autograder zipfile")
    assign_parser.add_argument("-r", "--requirements", nargs='?', default="requirements.txt", type=str, help="Path to requirements.txt file; ./requirements.txt automatically checked; use with --generate only")
    assign_parser.add_argument("--threshold", type=float, default=None, help="Pass/fail score threshold; use with --generate only")
    assign_parser.add_argument("--points", type=float, default=None, help="Points possible, overrides sum of test points; use with --generate only")
    assign_parser.add_argument("--seed", type=int, default=None, help="A random seed to be executed before each cell; use with --generate only")
    assign_parser.add_argument("--show-results", action="store_true", default=False, help="Show autograder test results (P/F only, no hints) after publishing grades (incl. hidden tests); use with --generate only")

    assign_parser.add_argument("files", nargs='*', help="Other support files needed for distribution (e.g. .py files, data files)")

    assign_parser.set_defaults(func=assign.main)


    ##### PARSER FOR otter check #####
    check_parser = subparsers.add_parser("check", description="Checks Python file against tests")
    check_parser.add_argument("file", help="Python file to grade")
    check_parser.add_argument("-q", "--question", help="Grade a specific test")
    check_parser.add_argument("-t", "--tests-path", default="tests", help="Path to test files")
    check_parser.add_argument("--seed", type=int, default=None, help="A random seed to be executed before each cell")

    check_parser.set_defaults(func=check.main)


    ##### PARSER FOR otter export #####
    export_parser = subparsers.add_parser("export", description="Exports a Jupyter Notebook to PDF with optional filtering")
    export_parser.add_argument("source", help="Notebook to export")
    export_parser.add_argument("dest", nargs='?', default=None, help="Path to write PDF")
    export_parser.add_argument("--filtering", default=False, action="store_true", help="Whether the PDF should be filtered")
    export_parser.add_argument("--pagebreaks", default=False, action="store_true", help="Whether the PDF should have pagebreaks between questions")
    export_parser.add_argument("--debug", default=False, action="store_true", help="Export in debug mode")

    export_parser.set_defaults(func=export.main)


    ##### PARSER FOR otter generate #####
    generate_parser = subparsers.add_parser("generate", description="Generates zipfile to configure Gradescope autograder")
    generate_subparsers = generate_parser.add_subparsers()


    ##### PARSER FOR otter generate autograder #####
    generate_autograder_parser = generate_subparsers.add_parser("autograder", description="Create an autograder zip file for Gradescope")
    generate_autograder_parser.add_argument("-t", "--tests-path", nargs='?', type=str, default="./tests/", help="Path to test files")
    generate_autograder_parser.add_argument("-o", "--output-path", nargs='?', type=str, default="./", help="Path to which to write zipfile")
    generate_autograder_parser.add_argument("-r", "--requirements", nargs='?', default="requirements.txt", type=str, help="Path to requirements.txt file; ./requirements.txt automatically checked")
    generate_autograder_parser.add_argument("--threshold", type=float, default=None, help="Pass/fail score threshold")
    generate_autograder_parser.add_argument("--points", type=float, default=None, help="Points possible, overrides sum of test points")
    generate_autograder_parser.add_argument("--show-results", action="store_true", default=False, help="Show autograder test results (P/F only, no hints) after publishing grades (incl. hidden tests)")
    generate_autograder_parser.add_argument("--seed", type=int, default=None, help="A random seed to be executed before each cell")
    generate_autograder_parser.add_argument("--token", default=None, help="Gradescope token for generating and uploading PDFs")
    generate_autograder_parser.add_argument("--unfiltered-pdfs", default=False, action="store_true", help="Whether the PDFs should be unfiltered")
    generate_autograder_parser.add_argument("--no-pagebreaks", default=False, action="store_true", help="Whether the PDFs should not have page breaks between questions")
    generate_autograder_parser.add_argument("--course-id", default=None, help="Gradescope course ID")
    generate_autograder_parser.add_argument("--assignment-id", default=None, help="Gradescope assignment ID for PDFs")
    generate_autograder_parser.add_argument("files", nargs='*', help="Other support files needed for grading (e.g. .py files, data files)")

    generate_autograder_parser.set_defaults(func=generate.autograder.main)


    ##### PARSER FOR otter generate token #####
    generate_token_parser = generate_subparsers.add_parser("token", description="Get a Gradescope token")

    generate_token_parser.set_defaults(func=generate.token.main)


    ##### PARSER FOR otter grade #####
    grade_parser = subparsers.add_parser("grade", description="Grade assignments locally using Docker containers")

    # necessary path arguments
    grade_parser.add_argument("-p", "--path", type=str, default="./", help="Path to directory of submissions")
    grade_parser.add_argument("-t", "--tests-path", type=str, default="./tests/", help="Path to directory of tests")
    grade_parser.add_argument("-o", "--output-path", type=str, default="./", help="Path to which to write output")

    # metadata parser arguments
    grade_parser.add_argument("-g", "--gradescope", action="store_true", default=False, help="Flag for Gradescope export")
    grade_parser.add_argument("-c", "--canvas", action="store_true", default=False, help="Flag for Canvas export")
    grade_parser.add_argument("-j", "--json", default=False, help="Flag for path to JSON metadata")
    grade_parser.add_argument("-y", "--yaml", default=False, help="Flag for path to YAML metadata")

    # script grading argument
    grade_parser.add_argument("-s", "--scripts", action="store_true", default=False, help="Flag to incidicate grading Python scripts")

    # PDF export options
    grade_parser.add_argument("--pdf", action="store_true", default=False, help="Create unfiltered PDFs for manual grading")
    grade_parser.add_argument("--tag-filter", action="store_true", default=False, help="Create a tag-filtered PDF for manual grading")
    grade_parser.add_argument("--html-filter", action="store_true", default=False, help="Create an HTML comment-filtered PDF for manual grading")

    # other settings and optional arguments
    grade_parser.add_argument("-f", "--files", nargs="+", help="Specify support files needed to execute code (e.g. utils, data files)")
    grade_parser.add_argument("-v", "--verbose", action="store_true", help="Flag for verbose output")
    grade_parser.add_argument("--seed", type=int, default=None, help="A random seed to be executed before each cell")
    grade_parser.add_argument("-r", "--requirements", default="requirements.txt", type=str, help="Flag for Python requirements file path; ./requirements.txt automatically checked")
    grade_parser.add_argument("--containers", type=int, help="Specify number of containers to run in parallel")
    grade_parser.add_argument("--image", default="ucbdsinfra/otter-grader", help="Custom docker image to run on")
    grade_parser.add_argument("--no-kill", action="store_true", default=False, help="Do not kill containers after grading")
    grade_parser.add_argument("--debug", action="store_true", default=False, help="Print stdout/stderr from grading for debugging")

    grade_parser.set_defaults(func=grade.main)


    ###### PARSER FOR otter service #####
    service_parser = subparsers.add_parser("service", description="Create and manage an otter-service")
    service_subparsers = service_parser.add_subparsers()


    ##### PARSER FOR otter service build #####
    service_build_parser = service_subparsers.add_parser("build", description="Build images for an otter-service instance")
    service_build_parser.add_argument("repo_path", default=".", help="Path to assignments repo root")
    service_build_parser.add_argument("--db-host", default="localhost", help="Postgres database host")
    service_build_parser.add_argument("--db-port", default=5432, type=int, help="Postgres database port")
    service_build_parser.add_argument("-u", "--db-user", default="root", help="Postgres database user")
    service_build_parser.add_argument("-p", "--db-pass", default="root", help="Postgres database password")
    service_build_parser.add_argument("--image", default="ucbdsinfra/otter-grader", help="Based image for grading containers")
    service_build_parser.add_argument("-q", "--quiet", default=False, action="store_true", help="Build images without writing Docker messages to stdout")

    service_build_parser.set_defaults(func=service.build.main)


    ##### PARSER FOR otter service create #####
    service_create_parser = service_subparsers.add_parser("create", description="Create database for otter-service")
    service_create_parser.add_argument("--db-host", default="localhost", help="Postgres database host")
    service_create_parser.add_argument("--db-port", default=5432, type=int, help="Postgres database port")
    service_create_parser.add_argument("-u", "--db-user", default="root", help="Postgres database user")
    service_create_parser.add_argument("-p", "--db-pass", default="root", help="Postgres database password")

    service_create_parser.set_defaults(func=service.create.main)


    ##### PARSER FOR otter service start #####
    service_start_parser = service_subparsers.add_parser("start", description="Start an otter-service instance")
    service_start_parser.add_argument("-c", "--config", help="Path to config file")
    service_start_parser.add_argument("-e", "--endpoint", help="Address of this VM including port")
    service_start_parser.add_argument("--port", type=int, default=80, help="Port for server to listen on")
    service_start_parser.add_argument("-k", "--google-key", help="Google OAuth key; use environment variable if not specified")
    service_start_parser.add_argument("-s", "--google-secret", help="Google OAuth secret; use environment variable if not specified")
    service_start_parser.add_argument("--db-host", default="localhost", help="Postgres database host")
    service_start_parser.add_argument("--db-port", default=5432, type=int, help="Postgres database port")
    service_start_parser.add_argument("-u", "--db-user", default="root", help="Postgres database user")
    service_start_parser.add_argument("-p", "--db-pass", default="root", help="Postgres database password")
    service_start_parser.add_argument("-l", "--rate-limit", default=120, type=int, help="Rate limit for submissions in seconds")

    service_start_parser.set_defaults(func=service.start.main)


    return parser