"""
This function is a parameterized SCD merging function. Its capabilities are defined using the Kimball Data Warehousing toolkit (Third Edition)
It is common for different attributes of a given dimension to have different tracking techniques

## SCD TYPE DEFINITIONS ##
Type 0: Retain original
-----------------------
With type 0, the dimension attribute value never changes, so facts are always grouped 
by this original value. Type 0 is appropriate for any attribute labeled “original,” such 
as a customer's original credit score or a durable identiﬁer. It also applies to most 
attributes in a date dimension. 

Example
-------
>>> df_source                   >>> df_target

     customer  credit_score          customer  original_credit_score
0   C00000001           730     0   C00000001                    630
1   C00000002           480     1   C00000002                    520

>>> scd_merge(df_source, df_target)

     customer  original_credit_score
0   C00000001                    630
1   C00000002                    520

Note: Though df_source has new values for credit_score, function retains the value found in df_target. Function also relabels the column as it is passed from df_source, signifying it is SCD0


Type 1: Overwrite
-----------------
With type 1, the old attribute value in the dimension row is overwritten with the new 
value; type 1 attributes always reﬂects the most recent assignment, and therefore 
this technique destroys history. Although this approach is easy to implement and 
does not create additional dimension rows, you must be careful that aggregate fact 
tables and OLAP cubes affected by this change are recomputed.

Example
-------
>>> df_source                   >>> df_target

     customer  credit_score          customer  current_credit_score
0   C00000001           730     0   C00000001                   630
1   C00000002           480     1   C00000002                   520

>>> scd_merge(df_source, df_target)

     customer  current_credit_score
0   C00000001                   730
1   C00000002                   480

Note: New values for credit score in df_source overwrite the value found in df_target. merge function assigns a tag to the column signifying SCD1 cahracter 'current_'

Type 2: Add new row
-------------------
Type 2  changes add a new row in the dimension with the updated attribute values. 
This requires generalizing the primary key of the dimension beyond the natural or 
durable key because there will potentially be multiple rows describing each member. 
When a new row is created for a dimension member, a new primary surrogate key is 
assigned and used as a foreign key in all fact tables from the moment of the update 
until a subsequent change creates a new dimension key and updated dimension row.
A minimum of three additional columns should be added to the dimension row 
with type 2 changes: 1) row effective date or date/time stamp; 2) row expiration 
date or date/time stamp; and 3) current row indicator.

Example
-------
>>> df_source                   >>> df_target

     customer  credit_score          customer  effective_from  effective_to  is_current  credit_score
0   C00000001           730     0   C00000001      2022-01-01    9999-12-31           1           630
1   C00000002           480     1   C00000002      2022-01-01    9999-12-31           1           520

>>> scd_merge(df_source, df_target) // date of merge in example is 2022-01-31

     customer  effective_from  effective_to  is_current  credit_score
0   C00000001      2022-01-01    2022-01-30           0           630
1   C00000001      2022-01-31    9999-12-31           1           730
2   C00000002      2022-01-01    9999-12-31           0           520
3   C00000002      2022-01-01    9999-12-31           1           480

Type 3: Add New Attribute
-------------------------
Type 3  changes add a new attribute in the dimension to preserve the old attribute 
value; the new value overwrites the main attribute as in a type 1 change. This kind of 
type 3 change is sometimes called an alternate reality. A business user can group and 
ﬁlter fact data by either the current value or alternate reality. This slowly changing 
dimension technique is used relatively infrequently.

Example
-------
>>> df_source                   >>> df_target

     customer  credit_score          customer credit_score  credit_score_alternate
0   C00000001           730     0   C00000001          630                    null
1   C00000002           480     1   C00000002          520                    null

>>> scd_merge(df_source, df_target) // date of merge in example is 2022-01-31

     customer credit_score  credit_score_alternate
0   C00000001          730                     630                     
1   C00000002          480                     520        

Note: does this keep going? should we KEEP adding columns?


Type 4: Add Mini-Dimension
--------------------------
The type 4  technique is used when a group of attributes in a dimension rapidly 
changes and is split off to a mini-dimension. This situation is sometimes called a 
rapidly changing monster dimension. Frequently used attributes in multimillion-row 
dimension tables are mini-dimension design candidates, even if they don't fre-
quently change. The type 4 mini-dimension requires its own unique primary key; 
the primary keys of both the base dimension and mini-dimension are captured in 
the associated fact tables.

Type 5: Add Mini-Dimention and Type 1 Outrigger
-----------------------------------------------
The type 5  technique is used to accurately preserve historical attribute values, 
plus report historical facts according to current attribute values. Type 5 builds on 
the type 4 mini-dimension by also embedding a current type 1 reference to the 
mini-dimension in the base dimension. This enables the currently-assigned mini-
dimension attributes to be accessed along with the others in the base dimension 
without linking through a fact table. Logically, you'd represent the base dimension 
and mini-dimension outrigger as a single table in the presentation area. The ETL 
team must overwrite this type 1 mini-dimension reference whenever the current 
mini-dimension assignment changes. 

Type 6: Add Type 1 attributes to Type 2 Dimension
-------------------------------------------------
Like  type 5, type 6 also delivers both historical and current dimension attribute 
values. Type 6 builds on the type 2 technique by also embedding current type 
1 versions of the same attributes in the dimension row so that fact rows can be 
ﬁltered or grouped by either the type 2 attribute value in effect when the measure-
ment occurred or the attribute's current value. In this case, the type 1 attribute is 
systematically overwritten on all rows associated with a particular durable key 
whenever the attribute is updated. 

Example
-------
>>> df_source                   >>> df_target

     customer  credit_score          customer  effective_from  effective_to  is_current  credit_score_current  credit_score_historical
0   C00000001           730     0   C00000001      2022-01-01    9999-12-31           1                   630                      630                         
1   C00000002           480     1   C00000002      2022-01-01    9999-12-31           1                   520                      520                         

>>> scd_merge(df_source, df_target) // date of merge in example is 2022-01-31

     customer  effective_from  effective_to  is_current  credit_score_current  credit_score_historical
0   C00000001      2022-01-01    2022-01-30           0                   730                      630
1   C00000001      2022-01-31    9999-12-31           1                   730                      730
2   C00000002      2022-01-01    9999-12-31           0                   480                      520
3   C00000002      2022-01-01    9999-12-31           1                   480                      480

Type 7: Dual Type 1 and Type 2 Dimensions
-----------------------------------------
Type 7 is the ﬁnal hybrid technique used to support both as-was and as-is report-
ing. A fact table can be accessed through a dimension modeled both as a type 1 
dimension showing only the most current attribute values, or as a type 2 dimen-
sion showing correct contemporary historical proﬁles. The same dimension table 
enables both perspectives. Both the durable key and primary surrogate key of the 
dimension are placed in the fact table. For the type 1 perspective, the current ﬂag 
in the dimension is constrained to be current, and the fact table is joined via the 
durable key. For the type 2 perspective, the current ﬂag is not constrained, and the 
fact table is joined via the surrogate primary key. These two perspectives would be 
deployed as separate views to the BI applications.

"""

def scd_merge(df_source, df_target, table_params: dict):

    return df_merged