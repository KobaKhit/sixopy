# -- coding: utf-8 --

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
    from urllib.parse import quote
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen,quote

import configparser
import csv

import _pickle as cPickle
import sys

def size_of_list(l):
	size = sys.getsizeof(cPickle.dumps(l))/1000000
	print('Size is {} MB'.format(size))
	return(size)

def chunkify(a, n):
	# https://stackoverflow.com/a/2135920/1354513
    k, m = divmod(len(a), n)
    return (list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))
    


def urlshortener(url):
	# Returns a list of shortened urls using the tinyurl api.
	# Args:
	# - url - string or a list. A url or a list of urls.

	ret = []
	count = 1
	if type(url) is str:
		data = urlopen("https://tinyurl.com/api-create.php?url="+quote(url))
		return(data.readlines()[0].decode())
	elif type(url) is list:
		for u in url:
			ret.append(urlshortener(u))
			# print(str(count) + "/" + str(len(url)))
			count+=1
		return(ret)

def parse_config(file):
	# Parses a configuration file
	# https://www.reddit.com/r/learnpython/comments/264ffw/what_is_the_pythonic_way_of_storing_credentials/
	# Args:
	# - file. Filepath

	config = configparser.RawConfigParser()
	config.read(file)
	return(config)

def read_csv(file,delimiter=','):
	# Read in a csv file.
	# Args:
	# - file - string. File path
	# - delimiter - string. 
	res=[]
	with open(file) as f:
		c = csv.reader(f,delimiter=',')
		res=[r for r in c]
	return(res)

def write_csv(file,data,mode = 'wb',delimiter = ','):
	# Write to a csv file.
	# Args:
	# - file - string. File path
	# - data - list of lists.
	# - mode - w,r,a, etc.
	# - delimiter - string. 
	with open(file, mode, newline="\n") as f:
		writer = csv.writer(f, delimiter=delimiter)
		for line in data:
			writer.writerow(line)

# def main():
# 	f = parse_file(r'C:\Users\kkhitalishvili\Desktop\creds.txt')
# 	print(f.get('azure','password'))



# if __name__ == "__main__":
# 	main()