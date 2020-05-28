import os
import json

class MapReduce(object):
	"""MapReduce class representing the mapreduce model"""

	def __init__(self, self, input_dir=settings.default_input_dir, output_dir=settings.default_output_dir,
                 n_mappers=settings.default_n_mappers, n_reducers=settings.default_n_reducers,
                 clean=True):
		super(MapReduce, self).__init__()
		self.input_dir = input_dir
        self.output_dir = output_dir
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.clean = clean

	def mapper(self, key, value):
        """outputs a list of key-value pairs, where the key is
        potentially new and the values are of a potentially different type.
        """
        pass

    def reduce(self, key, values_list):
        """Outputs a single value together with the provided key.
        Note: this function is to be implemented."""	

        pass

    def runMapper(self, index):
	    """Runs the implemented mapper    :param index: the index of the thread to run on
	    """
	    # read a key
	    # read a value
	    # get the result of the mapper
	    # store the result to be used by the reducer
	    pass

    def runReducer(self, index):
	    """Runs the implemented reducer    :param index: the index of the thread to run on
	    """
	    # load the results of the map
	    # for each key reduce the values
	    # store the results for this reducer 
	    pass

    def run(self):
	    """Executes the map and reduce operations    """
	    # initialize mappers list
	    mapWorkers = []
	    # initialize reducers list
	    rdcWorkers = []
	    # run the map step
	    for thread_id in range(self.n_mappers):
	        p = Process(target=self.run_mapper, args=(thread_id,))
	        p.start()
	        map_workers.append(p)
	    [t.join() for t in map_workers]
	    # run the reduce step
	    for thread_id in range(self.n_reducers):
	        p = Process(target=self.run_reducer, args=(thread_id,))
	        p.start()
	        map_workers.append(p)
	    [t.join() for t in rdc_workers]	


def begin_file_split(split_index, index):
	file_split = open(settings.get_input_split_file(split_index-1), "w+")
    file_split.write(str(index)




def splitFileToChunks(chunkNos, input_file_path):
	file_size = os.path.getsize(input_file_path)
	unit_size = file_size / chunkNos + 1
	original_file = open(input_file_path, "r")
	file_content = original_file.read()
	original_file.close()
    (index, current_split_index) = (1, 1)
    current_split_unit = begin_file_split(current_split_index, index)
        for character in file_content:
            current_split_unit.write(character)
            if self.is_on_split_position(character, index, unit_size, current_split_index):
                current_split_unit.close()
                current_split_index += 1
                current_split_unit = self.begin_file_split(current_split_index,index)
            index += 1
        current_split_unit.close()


def joinFiles(self, filesNos, clean = None, sort = True, decreasing = True):
	pass

