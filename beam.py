

#---------------The purpose of this code is to take the csv file from the BigQuery output and make transformations using Apache Beam, finally store the result in a JSON file------------------#
# steps to be followed before running this code
# 1) As Apache beam is in virtual env, run this: . ~/Documents/SearchTeam/ApacheBeam/bin/activate

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.textio import ReadFromText, WriteToText
import json

#-------------- Variables used ----------------#
jsonFile = {}
JSON_output_filename = 'user_item_price.json'
input_filename = 'user_item_price.csv'  # output of csv file from BigQuery


#------- User defined classes for Apache Beam
class collectData(beam.DoFn):
	def process(self,element):
		result = [ (element['bid'],element['iname'])]
		return result

class itemUserSplit(beam.DoFn):
    def process(self,element):
        user_id, item_id, price = element.split(',')
        return [{'uid':user_id, 'iid':item_id, 'price': (price)} ]

class adjustData(beam.DoFn):
    def process(self,element):
        result = [(element['uid'],(element['iid'],element['price']))]
        return result

class toDict(beam.DoFn):
    def process(self,ele):
        return [{ele[0]:ele[1]}]

class toJSON(beam.DoFn):
    def process(self,ele):
        for key, value in ele.items():
            jsonFile[key] = value

def runMe():
    with beam.Pipeline(options=PipelineOptions()) as p:
        rows = p | 'readData' >> ReadFromText(input_filename) | 'splitData' >> beam.ParDo(itemUserSplit()) | 'adjustData' >> beam.ParDo(adjustData()) | 'groupUsers' >> beam.GroupByKey()  # Get the items based on the user.
        rows | beam.ParDo(toDict()) | beam.ParDo(toJSON())   # Get the suggestions into JSON format from Dictionary format
    
    # Now we have the recommendation data in jsonFile, so we need to dump to a file and store it.
    resultString = json.dumps(jsonFile)
    with open(JSON_output_filename,'w') as outfile:
        json.dump(resultString,outfile)



# ------ Main Function ------#

if __name__=="__main__":
    runMe()









# ------------------ Work around space -------------------#

	#joinItems = rows | 'GroupByKey' >> beam.GroupByKey() #| 'joinItems ' >> beam.CombineValues(beam.combiners.CountCombineFn())
	#rows | 'print' >> beam.Map(collect)

	#joinItems | 'write' >> WriteToText(output_filename)

#print output
"""
    
    with beam.Pipeline(options=PipelineOptions()) as p:
        rows = p | 'readData' >> ReadFromText(input_filename) | 'splitData' >> beam.ParDo(itemUserSplit()) | 'adjustData' >> beam.ParDo(adjustData()) | 'groupUsers' >> beam.GroupByKey()
        data = rows | beam.ParDo(adjustData()) | 'groupUsers' >> beam.GroupByKey()
        
    

    
  rows |  beam.io.WriteToText('waste.txt')   -- for printing to file
  
  
  In [117]: class toDict(beam.DoFn):
  ...:     def process(self,ele):
  ...:         return [{ele[0]:ele[1]}]
  
  with beam.Pipeline(options=PipelineOptions()) as p:
       rows = p | 'readData' >> ReadFromText(input_filename) | 'splitData' >> beam.ParDo(itemUserSplit()) | 'adjustData' >> beam.ParDo(adjustData()) | 'groupUsers' >> beam.GroupByKey()
       output =  rows | beam.ParDo(toDict()) | beam.ParDo(toJSON())
       

       
       
       
       
       ---------testing-------
       class toDict(beam.DoFn):
        def process(self,ele):
            return {ele[0]:{ele[1]}}
            
            #p = beam.Pipeline(options=PipelineOptions())
            
            #### example 1 #########
            '''
            def addSum(line):
            line = int(line.strip())
            return line + 10
            
            with beam.Pipeline(options=PipelineOptions()) as p:
            lines = p | 'read' >> ReadFromText('input.txt')
            answer = lines | 'Sum' >> beam.Map(addSum)
            answer | 'Write' >> WriteToText('outputText.txt')
            
            
            
            
            class split(beam.DoFn):
            def process(self,element):
            buyer_id, buyer_name, item_name = element.split(',')
            return [{'bid':buyer_id, 'bname':buyer_name, 'iname': item_name} ]
            '''
            
            
            
            class WriteToCSV(beam.DoFn):
            def process(self,element):
            result = ["{},{}".format(element[0],element[1])]
            return result
            
            def collect(row):
            output.append(row)
            return True
    """
