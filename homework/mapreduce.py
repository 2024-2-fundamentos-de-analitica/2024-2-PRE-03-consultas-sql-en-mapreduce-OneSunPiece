"""Map/Reduce Implementation"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path

def _load_input(input_directory):
    sequence = []
    files = glob.glob(f"{input_directory}/*")
    with fileinput.input(files=files) as file:
        for line in file:
            sequence.append((fileinput.filename(),line))
    return sequence

def _shuffle_and_sort(sequence):
    return sorted(sequence,key=lambda x: x[0])

def _create_output_directory(output_directory):
    if os.path.exists(output_directory):
        os.system(f"rm -Rf {output_directory}")
    os.makedirs(output_directory)

def _save_output(output_directory,sequence):
    with open (f"{output_directory}/part-00000","w",encoding='UTF-8') as file:
        for key,value in sequence:
            file.write(f'{key}\t{value}')

def _create_marker(output_directory):
    with open(f"{output_directory}/_SUCCESS","w",encoding='UTF-8') as file:
        file.write("")

def run_mapreduce_job(mapper,reducer,input_directory,output_directory):
    """"Orchestrate all the jobs
    params:
        mapper: function to map the input
        reducer: function to reduce the output
        input_directory: directory where the input files are
        output_directory: directory where the output files will
    return:
        None
    """
    # 02. Load the input
    sequence = _load_input(input_directory)
    # 03. Mapper
    sequence = mapper(sequence)
    # 04. Shuffle and sort
    sequence = _shuffle_and_sort(sequence)
    # 05. reducer
    sequence = reducer(sequence)
    # 01. Create the output directory
    _create_output_directory(output_directory)
    # 06. Save the output
    _save_output(output_directory,sequence)
    # 07. Create the marker
    _create_marker(output_directory)