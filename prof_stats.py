import sys
import pstats
p = pstats.Stats('results.prof')
p.print_stats()
