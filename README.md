# Author: Kacem, Hamza
# My approach:
The optimization in my implementation is based on chain folding:
#### First optimization:
Application of rule 1 in chain folding:<br>
Mapper([M:Selection]) + Mapper([M:Rename]) => Mapper([M:Selection,M:Rename])
#### Second optimization:
Application of rule 2 in chain folding: <br>
Mapper([M:Selection]) + MapReduce([M:Joint] + [R:Joint]) => MapReduce([M:Selection, M:Joint] + [R:Joint])<br>
Mapper([M:Rename]) + Mapper([M:Selection]) + MapReduce([M:Joint] + [R:Joint]) => MapReduce([M:Rename, M:Selection, M:Joint] + [R:Joint])
#### Third optimization:
Application of rule 3 in chain folding: <br>
Already implemented in Milestone 3: Elimination of redundancy in a Projection Task.

