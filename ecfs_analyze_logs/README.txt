## Basic Preparation

analyze_obfuscated_merged_minified:
    Read the ecfs_logs_2012-2014_obfuscated_sorted.gz
    produces ecfs_trace_analysis_2012-2014.json

## Workload characterization

analyze_NEW_CATEGORIES_SIZE_JSON_DATA.py
    uses ecfs_trace_analysis_2012-2014.json
    creates graphs (Total bytes requested per month / total requests per month)


analyze_file_access_times.py
    reads ecfs_logs_2012-2014_obfuscated_sorted.gz
    creates ecfs_access_times_since_create.lines.txt.gz


visualize_file_access_times.py
    produces CDF graphs


analyze_file_access_counts.py
    reads ecfs_logs_2012-2014_obfuscated_sorted.gz


## User Session Analysis
    analyze_user_sessions.py
        prepare
            reads ecfs_logs_2012-2014_obfuscated_sorted.gz
            produces pickles & csv
        analyze
            reads pickles & csv
            produces json
    visualize_user_sessions.py
        reads json & pickles
        produces graphs
