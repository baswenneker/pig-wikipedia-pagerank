%DECLARE X 0.15

-- Load the lines into INP_DATA, the row format is <from_page_id>: <to_page_id_1> .. <to_page_id_N> 
INP_DATA = LOAD '/user/hue/wikipedia_pagerank_s.txt' as (line: chararray);

-- http://stackoverflow.com/questions/11287362/splitting-a-tuple-into-multiple-tuples-in-pig
-- First, split the line in spaces and add a column for each <to_page_id_N>
-- Input row = 1: 10 22
-- Output row = '1:', '10', '22'
INP_DATA = FOREACH INP_DATA GENERATE FLATTEN(STRSPLIT(line, ' ', 100));

-- A lot happens in the following command.
-- 1. SUBSTRING($0, 0, INDEXOF($0, ':')) as from
-- This line removes the ':' from the <from_page_id> at $0, ie. '1:' is turned into '1'.
-- 2. 1.0/(COUNT(TOBAG(*))-1) as outbound_pagerank,
-- This line computes the outbound pagerank	= 1.0/(number of columns of the row you iterate - 1).
--                                          = 1.0/(number of outgoing links of page <from_page_id>)
-- 3. FLATTEN(TOBAG(*)) as to
-- This line creates a row for each column.
-- Input of this line is: '1:', '10', '22'
-- Output (example pagerank): 
-- 	1, 0.45, '1:'
--	1, 0.89, '10'
--	1, 0.34, '22'
INP_DATA = FOREACH INP_DATA GENERATE 
	SUBSTRING($0, 0, INDEXOF($0, ':')) as (from_page_id: long), 
    1.0/(COUNT(TOBAG(*))-1) as outbound_pagerank,
	FLATTEN(TOBAG(*)) as to_page_id;

-- Now filter all rows where the to column contains the character ':'.
-- Input of this line is:
-- 	1, 0.45, '1:'
--	1, 0.89, '10'
--	1, 0.34, '22'
-- The output is:
--	1, 0.89, '10'
--	1, 0.34, '22'
FROM_TO = FILTER INP_DATA BY (INDEXOF(to_page_id, ':') == -1);

-- Group on <from_page_id.
-- The input is:
--	1, 0.89, '10'
--	1, 0.34, '22'
-- The output is:
--	1, ({1, 0.89, '10'}, {1, 0.34, '22'})
FROM_TO = (COGROUP FROM_TO BY to_page_id);

-- Finally, let's compute the pagerank!
-- For each row sum the pagerank of the outgoing pages' pageranks.
-- The input is:
--	1, ({1, 0.89, '10'}, {1, 0.34, '22'})
-- The output is:
--	1, 1.23
PAGERANK = FOREACH FROM_TO GENERATE
	group as from_page_id,
	(1-$X) + $X*SUM(FROM_TO.outbound_pagerank) as pagerank;
--DESCRIBE PAGERANK;
--DUMP PAGERANK; 

--JOIN A BY $0, B BY $0;
INP_TITLES = LOAD '/user/hue/titles-sorted_s.txt' as (title: chararray);
INP_TITLES = RANK INP_TITLES;

--PAGERANK = JOIN PAGERANK BY from_page_id, INP_TITLES BY $0;
describe PAGERANK;
DESCRIBE INP_TITLES;
--DUMP INP_TITLES; 

--PR = JOIN PAGERANK BY from_page_id, INP_TITLES BY $0;
--DESCRIBE PR;
