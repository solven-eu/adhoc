Feature: Simpler scenarios for Adhoc queries

 Scenario: A grand-total without filter
   Given Append rows
         | k  | v |
         | k1 | 1 |
   Given Register aggregator name=theSum column=k key=SUM
   When Query measure=theSum debug=true
   Then View contains
         |
         |