Name   : Naimisha Yadav Marri Ravi
**********************************************************
**********************************************************

Populate Data:

Considering Polygon with first and last coordinates to be same. In the given sample text files, last coordinates are not looped back to first.So, adding last coordinate to be same as first coordinate

**********************************************************
Querying Data:

In Query 1, as per the instance while passing polygon coordinates to query first vertex and last are not same.It throws error as the polygon format should have first and last coordinated to be same. So while querying in code, I handled by adding last coordinate that is same as first coordinate.  

There are some functions names that changed from version 5 to 8 (for instance : ST_GEOMETRYFromText)

