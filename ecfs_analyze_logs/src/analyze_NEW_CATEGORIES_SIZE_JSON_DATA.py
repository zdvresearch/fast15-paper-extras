#!/usr/bin/env python

__author__ = 'Federico G. Padua'

import matplotlib
matplotlib.use('Agg')

import json
import os
import numpy as np
import matplotlib.pyplot as plt
import argparse

#plt.rc('text', usetex=True)
#plt.rc('font', **{'family': 'serif', 'serif': ['Computer Modern']})
#plt.rc('text', usetex=True)

def argparse_of_program():
    parser = argparse.ArgumentParser(description='This script generates plots of different things', version='0.1')

    parser.add_argument('-i', '--input', help='Input file name: json file with data', required=True)
    parser.add_argument('-d', '--dir', help='Output directory where plots will be placed', required=True)
    args = parser.parse_args()
    return args


def get_file_size_group_name(group_id):
    ffrom = 2**group_id
    fto = 2**(group_id+1)
    return '%dKB-%dKB' % (ffrom, fto)


#########################
    ##### plot on the same plot count of del, put, get per month (x axis = months, y axis = count)
def plot_delgetput_total_count_permonth(del_array, put_array, get_array, tags_r, x_axis_r, outfilename_j):

    plt.figure(figsize=(tags_r.__len__(), 11))
    plt.xticks(x_axis_r, tags_r, rotation=70)
    plt.yticks(fontsize=20)
    #plt.title('# Operations per month', fontsize=30)
    #plt.xlabel('Months', fontsize=20)
    plt.ylabel('# operations ($\cdot 10^6$)', fontsize=20)
    plt.grid(True)

    plt.plot(x_axis_r, del_array, color='0', label='delete', marker='o', linestyle='-', linewidth=2)
    plt.plot(x_axis_r, put_array, color='0', label='put', marker='s', linestyle='--', linewidth=2)
    plt.plot(x_axis_r, get_array, color='0', label='get', marker='x', linestyle=':', linewidth=2)
    plt.legend(loc='upper right', fontsize=20)
    plt.subplots_adjust(top=.91)
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
    #plt.show()
    plt.savefig(outfilename_j)
    plt.close()


#########################
    ##### plot on the same plot bytes of del, put, get per month (x axis = months, y axis = count)
def plot_delgetput_total_bytes_permonth(del_array, put_array, get_array, tags_r, x_axis_r):

    plt.figure(figsize=(tags_r.__len__(), 11))
    plt.xticks(x_axis_r, tags_r, rotation=70)
    plt.yticks(fontsize=20)
    #plt.title('# Operations per month', fontsize=30)
    #plt.xlabel('Months', fontsize=20)
    plt.ylabel('MB moved', fontsize=20)
    plt.grid(True)

    plt.plot(x_axis_r, del_array, color='0', label='delete', marker='o', linestyle='-', linewidth=2)
    plt.plot(x_axis_r, put_array, color='0', label='put', marker='s', linestyle='--', linewidth=2)
    plt.plot(x_axis_r, get_array, color='0', label='get', marker='x', linestyle=':', linewidth=2)
    plt.legend(loc='upper right', fontsize=20)
    plt.subplots_adjust(top=.91)
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
    #plt.show()
    plt.savefig('bytes_del_get_put_foreverymonth.pdf')
    plt.close()


def plot_bigplot_getplusput(gray_scale_r, x_axis_r, variable_r_get, variable_r_put, width_r, tags_r, list_of_ranges_r, outplotfilename, legend_labels, y_lab):
    # aggiungere outputfilename come argomento alla funzione
    fig = plt.figure(figsize=(tags_r.__len__(), 11), facecolor='white')
    ax = fig.add_subplot(111)

    space_btw_adiacent_bars = 0.08
    offset_bars = space_btw_adiacent_bars/2

    get_str = 'G'
    put_str = 'P'
    #colors ='rgbwmc'
    # custom colours
    #
    colors = ['#2166ac',
              '#fee090',
              '#fdbb84',
              '#fc8d59',
              '#e34a33',
              '#b30000']
    patch_handles_get = []
    patch_handles_put = []
    #plt.grid(True)
    ax.yaxis.grid(True)
    ax.xaxis.grid(False)


    #bottom = np.zeros(len(tags_r))  # bottom alignment of data starts at zero
    for month in range(0, len(tags_r)):
        bottom = 0
        for i, d in enumerate(variable_r_get[month]):
            patch_handles_get.append(ax.bar(x_axis_r[month]-width_r-offset_bars, d, width_r, color=colors[i % len(colors)], bottom=bottom))
            #patch_handles_get.append(ax.bar(x_axis_r[month]-width_r-offset_bars, d, width_r, color=gray_scale_r[i % len(gray_scale_r)], bottom=bottom))  ## gray scale
        # accumulate the left-hand offsets
            bottom += d
        height = bottom
        ax.text(x_axis_r[month]-width_r-offset_bars+width_r/2., 1.01*height, '%s' % get_str,
                ha='center', va='bottom', fontsize=13)

### now plot the put
    for month in range(0, len(tags_r)):
        bottom = 0
        for i, d in enumerate(variable_r_put[month]):
            patch_handles_put.append(ax.bar(x_axis_r[month]+offset_bars, d, width_r, color=colors[i % len(colors)], bottom=bottom))
            #patch_handles_put.append(ax.bar(x_axis_r[month]+offset_bars, d, width_r, color=gray_scale_r[i % len(gray_scale_r)], bottom=bottom))   ## gray scale
            # accumulate the left-hand offsets
            bottom += d
        #autolabel(patch_handles_put[month], put_str)
        height = bottom
        ax.text(x_axis_r[month]+offset_bars+width_r/2., 1.01*height, '%s' % put_str,
                ha='center', va='bottom', fontsize=13)

# # go through all of the bar segments and annotate
#    for j in xrange(0, len(patch_handles_get), 6):
#        print j
#         for i, patch in enumerate(patch_handles[j].get_children()):
#             print i
#             print patch
#             bl = patch.get_xy()
#             print bl
#             x = 0.5*patch.get_width() + bl[0]
#             y = 0.5*patch.get_height() + bl[1]
#             print x, y
#             ax.text(x, y, "%d%%" % (int(variable_r_get[j][i])), ha='center')

    ### box for some text
    textstr = 'G = GET\nP = PUT'
    #props = dict(boxstyle='round', facecolor='white', alpha=0.5)
    props = dict(boxstyle='square', facecolor='white')
    #ax.text(1.04, 0.70, textstr, transform=ax.transAxes, fontsize=13, verticalalignment='center', bbox=props)
    ax.text(1.045, 0.70, textstr, transform=ax.transAxes, fontsize=13, verticalalignment='center', bbox=props)

    plt.xticks(x_axis_r, tags_r, rotation=70)
    # define y axis ticks
    plt.yticks(np.arange(0, 105, 10), fontsize=16)
    # remove top and right line of plot frame, from:
    # http://stackoverflow.com/questions/9126838/how-to-simultaneously-remove-top-and-right-axes-and-plot-ticks-facing-outwards
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.tick_params(axis='both', direction='out')
    ax.get_xaxis().tick_bottom()   # remove unneeded ticks
    ax.get_yaxis().tick_left()
    ######## comment the above 5 lines if yo want top and right axis....
    plt.ylabel(y_lab, fontsize=21)
    ax.legend([patch_handles_get[0], patch_handles_get[1], patch_handles_get[2], patch_handles_get[3], patch_handles_get[4], patch_handles_get[5]], legend_labels, fancybox=True, bbox_to_anchor=(1.1, 1), loc='upper right', fontsize=13)
    #plt.show()
    plt.savefig(outplotfilename)
    plt.close()


def plot_bigplot_getplusput_absolute_numbers(gray_scale_r, x_axis_r, variable_r_get, variable_r_put, width_r, tags_r, list_of_ranges_r, outplotfilename, legend_labels, y_lab):
    # aggiungere outputfilename come argomento alla funzione
    fig = plt.figure(figsize=(tags_r.__len__(), 11), facecolor='white')
    ax = fig.add_subplot(111)

    space_btw_adiacent_bars = 0.08
    offset_bars = space_btw_adiacent_bars/2

    get_str = 'G'
    put_str = 'P'
    #colors ='rgbwmc'
    # custom colours
    #
    colors = ['#2166ac',
              '#fee090',
              '#fdbb84',
              '#fc8d59',
              '#e34a33',
              '#b30000']
    patch_handles_get = []
    patch_handles_put = []
    #plt.grid(True)
    ax.yaxis.grid(True)
    ax.xaxis.grid(False)
    #bottom = np.zeros(len(tags_r))  # bottom alignment of data starts at zero
    for month in range(0, len(tags_r)):
        bottom = 0
        for i, d in enumerate(variable_r_get[month]):
            patch_handles_get.append(ax.bar(x_axis_r[month]-width_r-offset_bars, d, width_r, color=colors[i % len(colors)], bottom=bottom))
            #patch_handles_get.append(ax.bar(x_axis_r[month]-width_r-offset_bars, d, width_r, color=gray_scale_r[i % len(gray_scale_r)], bottom=bottom))  ## gray scale
        # accumulate the left-hand offsets
            bottom += d
        height = bottom
        ax.text(x_axis_r[month]-width_r-offset_bars+width_r/2., 1.01*height, '%s' % get_str,
                ha='center', va='bottom', fontsize=17)

### now plot the put
    for month in range(0, len(tags_r)):
        bottom = 0
        for i, d in enumerate(variable_r_put[month]):
            patch_handles_put.append(ax.bar(x_axis_r[month]+offset_bars, d, width_r, color=colors[i % len(colors)], bottom=bottom))
            #patch_handles_put.append(ax.bar(x_axis_r[month]+offset_bars, d, width_r, color=gray_scale_r[i % len(gray_scale_r)], bottom=bottom))   ## gray scale
            # accumulate the left-hand offsets
            bottom += d
        #autolabel(patch_handles_put[month], put_str)
        height = bottom
        ax.text(x_axis_r[month]+offset_bars+width_r/2., 1.01*height, '%s' % put_str,
                ha='center', va='bottom', fontsize=17)

# # go through all of the bar segments and annotate
#    for j in xrange(0, len(patch_handles_get), 6):
#        print j
#         for i, patch in enumerate(patch_handles[j].get_children()):
#             print i
#             print patch
#             bl = patch.get_xy()
#             print bl
#             x = 0.5*patch.get_width() + bl[0]
#             y = 0.5*patch.get_height() + bl[1]
#             print x, y
#             ax.text(x, y, "%d%%" % (int(variable_r_get[j][i])), ha='center')

    ### box for some text
    textstr = 'G = GET\nP = PUT'
    #props = dict(boxstyle='round', facecolor='white', alpha=0.5)
    props = dict(boxstyle='square', facecolor='white')
    #ax.text(1.045, 0.70, textstr, transform=ax.transAxes, fontsize=13, verticalalignment='center', ha='center', bbox=props)  ## this is outside the plot
    ax.text(0.25, 0.83, textstr, transform=ax.transAxes, fontsize=17, verticalalignment='center', ha='center', bbox=props)


    plt.xticks(x_axis_r, tags_r, rotation=70, fontsize=17)
    #plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))   # use scientific notation
    # define y axis ticks
    #plt.yticks(np.arange(0, 105, 10), fontsize=16)
    # remove top and right line of plot frame, from:
    # http://stackoverflow.com/questions/9126838/how-to-simultaneously-remove-top-and-right-axes-and-plot-ticks-facing-outwards
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.tick_params(axis='both', direction='out')
    ax.tick_params(axis='y', labelsize=19)
    ax.get_xaxis().tick_bottom()   # remove unneeded ticks
    ax.get_yaxis().tick_left()
    ######## comment the above 5 lines if yo want top and right axis....
    plt.ylabel(y_lab, fontsize=23)
    #legend = ax.legend([patch_handles_get[0], patch_handles_get[1], patch_handles_get[2], patch_handles_get[3], patch_handles_get[4], patch_handles_get[5]], legend_labels, fancybox=False, loc='upper left', fontsize=20)  #, bbox_to_anchor=(1.1, 1))
    legend = ax.legend([patch_handles_get[5], patch_handles_get[4], patch_handles_get[3], patch_handles_get[2], patch_handles_get[1], patch_handles_get[0]], legend_labels[::-1], fancybox=False, loc='upper left', fontsize=20)  #, bbox_to_anchor=(1.1, 1))
    #plt.show()
    #legend.draw_frame(False)
    plt.tight_layout()
    plt.savefig(outplotfilename)
    plt.close()



def plot_percentagereq_for_differen_sizes_forasinglemonth(gray_scale_r, variable_r, width_r, list_of_ranges_r, outplotfile, y_label):

    x_axis_new = []
    for i in range(0, len(list_of_ranges_r)):
        x_axis_new.append(i+1)
    plt.figure(figsize=(list_of_ranges_r.__len__(), 11))
    plt.xticks(x_axis_new, list_of_ranges_r, rotation=70)
    plt.yticks(fontsize=12)
    plt.grid(True)

    temp_list = []

    for ciao in range(0, len(list_of_ranges_r), 1):
        temp_list.append(variable_r[ciao][0])  # change second index to change the month: 0,1,2,...23

    total_requests_in_this_month = float(sum(temp_list))

    print("max of counts %d" % max(temp_list))
    maxim = float(max(temp_list))

    new_list_normalized = []
    #for zeta in range(0, len(list_of_ranges_r), 1):
    for zeta in temp_list:
        new_list_normalized.append(float(zeta/total_requests_in_this_month)*100)

    #print(temp_list)
    #print(new_list_normalized)

    plt.ylim([0, 100])
    #total_get_requests_for_given_month
    for babbo in range(0, len(list_of_ranges_r), 1):
    #    plt.bar(x_axis_new[babbo], variable_r[babbo][0], width_r, color=gray_scale_r[babbo], label=list_of_ranges_r[babbo])

        plt.bar(x_axis_new[babbo], new_list_normalized[babbo], width_r, color=gray_scale_r[babbo], label=list_of_ranges_r[babbo])



    #plt.legend(loc='upper left', fontsize=11)
    #plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
    #plt.ylabel('# GET operations ($\cdot 10^6$)', fontsize=20)
    plt.ylabel(y_label, fontsize=15)
    plt.subplots_adjust(bottom=.20)
    #plt.show()

    plt.savefig(outplotfile)
    plt.close()


#def plot_bigplot_getplusput_tape_and_disk(gray_scale_r, x_axis_r, variable_r_get, variable_r_put, width_r, tags_r, list_of_ranges_r, outplotfilename, legend_labels, y_lab):   ##### tape and disk comparison for get and put... bytes and # requests
#    pass


def get_normalized_list_for_every_month(variable_r, list_of_ranges_r, tags_r):

    """
    :param variable_r: big list with all the data [sizes][months]
    :param list_of_ranges_r: sorted list of range (sizes...Enormous, etc.)
    :return: normalized list for each month (numbers are percentage respect to the total bytes/requests in a given month)
    """

    number_of_months = len(tags_r)
    temp_list = [[] for lil in range(0, number_of_months)]
    total_requests_in_each_month = [[] for lil in range(0, number_of_months)]
    maxima_each_month = [[] for lil in range(0, number_of_months)]
    new_list_normalized = [[] for lil in range(0, number_of_months)]

    for month in range(0, number_of_months):
        for ciao in range(0, len(list_of_ranges_r), 1):
            temp_list[month].append(variable_r[ciao][month])  # change second index to change the month: 0,1,2,...23

    for month in range(0, number_of_months):
        total_requests_in_each_month[month] = float(sum(temp_list[month]))

    #print("total bytes requested in month 0: %f" % total_requests_in_each_month[0])

    # list of maxima for each month
    for month in range(0, number_of_months):
        maxima_each_month[month] = max(temp_list[month])

    #print("maxima for the first month: %d", maxima_each_month[0])
    for month in range(0, number_of_months):
        for zeta in temp_list[month]:
            new_list_normalized[month].append((zeta/total_requests_in_each_month[month])*100)

    return new_list_normalized


def modify_list_absolute_numbers(variable_r, list_of_ranges_r, tags_r):

    number_of_months = len(tags_r)
    temp_list = [[] for lil in range(0, number_of_months)]
    #total_requests_in_each_month = [[] for lil in range(0, number_of_months)]
    #maxima_each_month = [[] for lil in range(0, number_of_months)]
    #new_list_normalized = [[] for lil in range(0, number_of_months)]

    for month in range(0, number_of_months):
        for ciao in range(0, len(list_of_ranges_r), 1):
            temp_list[month].append(variable_r[ciao][month])  # change second index to change the month: 0,1,2,...23
    return temp_list


def plot_avg_time_of_operation_per_month(list_of_values, x_axis_r, tags_r, width_r, y_label_r, outfilename_p):

    plt.figure(figsize=(list_of_values.__len__(), 11))
    plt.xticks(x_axis_r, tags_r, rotation=70)
    plt.yticks(fontsize=12)
    plt.grid(True)

    for babbo in range(0, len(list_of_values), 1):
        plt.bar(x_axis_r[babbo], list_of_values[babbo], width_r)   #, color=gray_scale_r[babbo], label=list_of_ranges_r[babbo])

    plt.ylabel(y_label_r, fontsize=15)
    #plt.subplots_adjust(bottom=.20)
    #plt.show()

    plt.savefig(outfilename_p)
    plt.close()


if __name__ == '__main__':

    args_program = argparse_of_program()

    print("===== This program extract info from json file with data summary =====")
    # read in the json file
    with open (args_program.input, 'rb') as inp:
        df = json.load(inp)  # pd.read_json(args_program.input)
    output_directory_for_plots = args_program.dir

    # build some arrays for later plotting
    del_count_per_month = []
    get_total_count_per_month = []
    put_total_count_per_month = []
    #get_total_bytes_disk_per_month = []
    #get_total_bytes_tape_per_month = []
    #put_total_bytes_disk_per_month = []
    #put_total_bytes_tape_per_month = []

    ####  average execution time to complete a requests

    ### GET
    get_avg_exec_time_per_month_disk = []
    get_avg_exec_time_per_month_tape = []

    #### PUT
    put_avg_exec_time_per_month = []

    ### total amount of time spent satisfying the given request

    get_total_exec_time_per_month_disk = []
    get_total_exec_time_per_month_tape = []

    #### PUT

    put_total_exec_time_per_month = []

    # GET operation : % of average time spent satisfying a request from tape and disk


    check_append = []
    tags = []
    x_axis = []
    i = 0
    da = 0

    ## INITIALIZE a list of lists: compact way
    get_list_bytes_bysize = [[] for i in range(0, 6)]  ### get[size_range][months]
    put_list_bytes_bysize = [[] for i in range(0, 6)]  ### put[size_range][months]

    get_list_numrequests_bysize = [[] for i in range(0, 6)]  ### get[size_range][months]
    put_list_numrequests_bysize = [[] for i in range(0, 6)]  ### put[size_range][months]

    #zipped_rangelist_variablenames = zip(variables_num_requests_per_size_total, list_of_ranges)
    size_range_key = []

    # some definitions...

    KB = 1024
    MB = KB * 1024
    GB = MB * 1024
    TB = GB * 1024
    print(TB)

    kilo = 1000
    million = kilo * 1000
    billion = million * 1000

    ##### NEW CATEGORIES
    GROUPS = [
        ("Tiny", 0, 128*KB),
        ("Small", 128*KB, 1*MB),
        ("Medium", 1*MB, 8*MB),
        ("Large", 8*MB, 128*MB),
        ("Huge", 128*MB, 1*GB),
        ("Enormous", 1*GB, 100000*TB)
    ]

    ECMWF_GROUPS = [
        ("Tiny", 0, 512*KB),
        ("Small", 512*KB, 1*MB),
        ("Medium", 1*MB, 8*MB),
        ("Large", 8*MB, 48*MB),
        ("Huge", 48*MB, 1*GB),
        ("Enormous", 1*GB, 100000*TB)
    ]

    # change GROUPS into  ECMWF_GROUPS if yo want to classify ranges as ECMWF, change leg_labels to leg_labels_ecmwf

    labels_list = []
    for ha in range(0, len(ECMWF_GROUPS)):
        temp_string = str(ECMWF_GROUPS[ha][1])+"-"+str(ECMWF_GROUPS[ha][2])
        labels_list.append(temp_string)

    leg_labels = ['0 - 128KB', '128KB - 1MB', '1MB - 8MB', '8MB - 128MB', '128MB - 1GB', '1GB - 100PB']
    leg_labels_ecmwf = ['Tiny (0 - 512KB)', 'Small (512KB - 1MB)', 'Medium (1MB - 8MB)', 'Large (8MB - 48MB)', 'Huge (48MB - 1GB)', 'Enormous (1GB - 32GB)']



    #### looping through the range names in th elist of tuples
    #for (range_name, start_of_range, end_of_range) in GROUPS:
    #    print("%s" % range_name)

    ## create a list to hold the alphabetically sorted range names
    range_names = []
    for (range_name, start_of_range, end_of_range) in ECMWF_GROUPS:
        range_names.append(range_name)
    sorted_list_of_range_names = sorted(range_names)


    ## could create methods to get the info I want from the json....

    # loop through the dicts and subdicts to get what yo want...

    #
    for maxkey, subdictionary in df.iteritems():
        if len(maxkey) == 7 and '2011' not in maxkey:
            tags.append(maxkey)
            da += 1
            x_axis.append(da)
            for operationskey, subdict2 in subdictionary.iteritems():
                if 'get' in operationskey:
                    for by_size_total_key, subdict3 in subdict2.iteritems():
                        if 'by_size_total' in by_size_total_key:
                            for num_requests_key, subdict4 in subdict3.iteritems():
                                if 'size' in num_requests_key:
                                    for ciaociao, subdict5 in subdict4.iteritems():
                                        if 'ecmwf' in ciaociao:

                                   #### indent all this block until h += 1
                                            h = 0
                                    #for size_range_key in subdict4.iteritems():
                                            for size_range_key in sorted_list_of_range_names:
                                        #print type(size_range_key)
                                        #print("%s" % size_range_key[0])
                                        #set_of_ranges.add(size_range_key[0])

                                                if size_range_key in df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['%s' % num_requests_key]['%s' % ciaociao]:
                                                    get_list_bytes_bysize[h].append(float((df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['%s' % num_requests_key]['%s' % ciaociao]['%s' % size_range_key]['sum'])/TB))
                                                    get_list_numrequests_bysize[h].append(float((df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['%s' % num_requests_key]['%s' % ciaociao]['%s' % size_range_key]['count'])/kilo))
                                                else:
                                                    get_list_bytes_bysize[h].append(0)
                                                    get_list_numrequests_bysize[h].append(0)

                                                h += 1
                        elif 'requests_total' in by_size_total_key:
                            get_total_count_per_month.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['count'])
                        elif 'execution_time_disk' in by_size_total_key:
                            get_avg_exec_time_per_month_disk.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['avg'])
                            get_total_exec_time_per_month_disk.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['sum'])
                        elif 'execution_time_tape' in by_size_total_key:
                            get_avg_exec_time_per_month_tape.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['avg'])
                            get_total_exec_time_per_month_tape.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['sum'])

                elif 'put' in operationskey:
                    for by_size_total_key, subdict3 in subdict2.iteritems():
                        if 'by_size' in by_size_total_key:
                            for num_requests_key, subdict4 in subdict3.iteritems():
                                if 'size' in num_requests_key:
                                    for ciaociao, subdict5 in subdict4.iteritems():
                                        if 'ecmwf' in ciaociao:

                                            h = 0
                                    #for size_range_key in subdict4.iteritems():
                                            for size_range_key in sorted_list_of_range_names:
                                        #print type(size_range_key)
                                        #print("%s" % size_range_key[0])
                                        #set_of_ranges.add(size_range_key[0])

                                                if size_range_key in df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['%s' % num_requests_key]['%s' % ciaociao]:
                                                    put_list_bytes_bysize[h].append(float((df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['%s' % num_requests_key]['%s' % ciaociao]['%s' % size_range_key]['sum'])/TB))
                                                    put_list_numrequests_bysize[h].append(float((df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['%s' % num_requests_key]['%s' % ciaociao]['%s' % size_range_key]['count'])/kilo))
                                                else:
                                                    put_list_bytes_bysize[h].append(0)
                                                    put_list_numrequests_bysize[h].append(0)
                                                h += 1

                        elif 'requests' in by_size_total_key:
                            put_total_count_per_month.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['count'])
                        elif 'execution_time' in by_size_total_key:
                            put_avg_exec_time_per_month.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['avg'])
                            put_total_exec_time_per_month.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['sum'])

                elif 'del' in operationskey:
                    for by_size_total_key, subdict3 in subdict2.iteritems():
                        if 'requests' in by_size_total_key:
                            del_count_per_month.append(df['%s' % maxkey]['%s' % operationskey]['%s' % by_size_total_key]['count'])

    start_point = 0.35
    step_size = (1. - start_point) / len(sorted_list_of_range_names)
    gray_scale = []

    #for w in range(0, len(list_of_ranges)):
    #    gray_scale.append('%s' % (start_point + w * step_size))

    for w in range(0, len(ECMWF_GROUPS)):
        gray_scale.append('%s' % (start_point + w * step_size))

    #######################
    ###### plot all months, y axis num requests, x axis month , stacked bar plot with range sizes
    width = 0.28   ## 0.2 is fine....

    #print("printing x axis")
    #print("%d" % len(x_axis))
    #print(x_axis)

    y_axis_labels = [
        #'bytes requested within a certain size range',
        'TB requested within a certain size range',
        '# requests within a certain size range (x1000)',
        'bytes requested within a certain size range (%)',
        '# requests within a certain size range (%)',
        'average time to satisfy a GET request (s/ms/?) disk',
        'total time to satisfy all GET requests (s/ms/?) disk',
        'average time to satisfy a GET request (s/ms/?) tape',
        'total time to satisfy all GET requests (s/ms/?) tape',
        'average time to satisfy a PUT request (s/ms/?)',
        'total time to satisfy all PUT requests (s/ms/?)'
    ]

    #plot_numrequests_permonth_stackedbysize(gray_scale, x_axis, variables_num_requests_per_size_total, width, tags, list_of_ranges, outplotname_numrequests_permonth_stackedbysize)
    #plot_percentagereq_for_differen_sizes_forasinglemonth(gray_scale, get_list_bytes_bysize, width, sorted_list_of_range_names, outplotfile_percent_monthreq_bysize_GET, y_axis_labels[0])
    #plot_percentagereq_for_differen_sizes_forasinglemonth(gray_scale, put_list_bytes_bysize, width, sorted_list_of_range_names, outplotfile_percent_monthreq_bysize_PUT, y_axis_labels[1])
    normalized_list_for_a_given_month_GET = []
    normalized_list_for_a_given_month_PUT = []
    normalized_list_for_a_given_month_GET_num_req = []
    normalized_list_for_a_given_month_PUT_num_req = []

    #normalized_list_for_a_given_month_GET = get_normalized_list_for_every_month(get_list_bytes_bysize, sorted_list_of_range_names, tags)
    #normalized_list_for_a_given_month_PUT = get_normalized_list_for_every_month(put_list_bytes_bysize, sorted_list_of_range_names, tags)

    #normalized_list_for_a_given_month_GET_num_req = get_normalized_list_for_every_month(get_list_numrequests_bysize, sorted_list_of_range_names, tags)
    #normalized_list_for_a_given_month_PUT_num_req = get_normalized_list_for_every_month(put_list_numrequests_bysize, sorted_list_of_range_names, tags)

    ##### CAN CREATE A LIST OF FILENAMES...
    list_output_filenames = [
        #os.path.join(output_directory_for_plots, 'percentbytes_permonth_stackedbysize_get_put_big_plot.pdf'),
        #os.path.join(output_directory_for_plots, 'percent_num_requests_permonth_stackedbysize_get_put_big_plot.pdf'),
        os.path.join(output_directory_for_plots, 'Count_per_month_total_requests_getputdel.pdf'),
        os.path.join(output_directory_for_plots, 'bytes_requested_absolute_values.pdf'),
        os.path.join(output_directory_for_plots, 'number_requests_absolute_values.pdf'),
        os.path.join(output_directory_for_plots, 'avg_time_put_distr.pdf'),
        os.path.join(output_directory_for_plots, 'total_time_put_distr.pdf'),
        os.path.join(output_directory_for_plots, 'avg_time_distr_get_disk.pdf'),
        os.path.join(output_directory_for_plots, 'total_time_distr_get_disk.pdf'),
        os.path.join(output_directory_for_plots, 'avg_time_distr_get_tape.pdf'),
        os.path.join(output_directory_for_plots, 'total_time_distr_get_tape.pdf')
    ]

    plot_delgetput_total_count_permonth(del_count_per_month, put_total_count_per_month, get_total_count_per_month, tags, x_axis, list_output_filenames[0])

    #### get plus put percentages: bytes and number requests

    #plot_bigplot_getplusput(gray_scale, x_axis, normalized_list_for_a_given_month_GET, normalized_list_for_a_given_month_PUT, width, tags, sorted_list_of_range_names, list_output_filenames[0], leg_labels_ecmwf[::-1], y_axis_labels[2])
    #plot_bigplot_getplusput(gray_scale, x_axis, normalized_list_for_a_given_month_GET_num_req, normalized_list_for_a_given_month_PUT_num_req, width, tags, sorted_list_of_range_names, list_output_filenames[1], leg_labels_ecmwf[::-1], y_axis_labels[3])

    list_for_a_given_month_GET = []
    list_for_a_given_month_PUT = []
    list_for_a_given_month_GET_num_req = []
    list_for_a_given_month_PUT_num_req = []

    list_for_a_given_month_GET = modify_list_absolute_numbers(get_list_bytes_bysize, sorted_list_of_range_names, tags)
    list_for_a_given_month_PUT = modify_list_absolute_numbers(put_list_bytes_bysize, sorted_list_of_range_names, tags)
    list_for_a_given_month_GET_num_req = modify_list_absolute_numbers(get_list_numrequests_bysize, sorted_list_of_range_names, tags)
    list_for_a_given_month_PUT_num_req = modify_list_absolute_numbers(put_list_numrequests_bysize, sorted_list_of_range_names, tags)


    #### get plus put absolute values: bytes and number requests
    plot_bigplot_getplusput_absolute_numbers(gray_scale, x_axis, list_for_a_given_month_GET, list_for_a_given_month_PUT, width, tags, sorted_list_of_range_names, list_output_filenames[1], leg_labels_ecmwf[::-1], y_axis_labels[0])
    plot_bigplot_getplusput_absolute_numbers(gray_scale, x_axis, list_for_a_given_month_GET_num_req, list_for_a_given_month_PUT_num_req, width, tags, sorted_list_of_range_names, list_output_filenames[2], leg_labels_ecmwf[::-1], y_axis_labels[1])

    plot_avg_time_of_operation_per_month(put_avg_exec_time_per_month, x_axis, tags, width, y_axis_labels[8], list_output_filenames[3])
    plot_avg_time_of_operation_per_month(put_total_exec_time_per_month, x_axis, tags, width, y_axis_labels[9], list_output_filenames[4])
    ##### GET
    plot_avg_time_of_operation_per_month(get_avg_exec_time_per_month_disk, x_axis, tags, width, y_axis_labels[4], list_output_filenames[5])
    plot_avg_time_of_operation_per_month(get_total_exec_time_per_month_disk, x_axis, tags, width, y_axis_labels[5], list_output_filenames[6])
    plot_avg_time_of_operation_per_month(get_avg_exec_time_per_month_tape, x_axis, tags, width, y_axis_labels[6], list_output_filenames[7])
    plot_avg_time_of_operation_per_month(get_total_exec_time_per_month_tape, x_axis, tags, width, y_axis_labels[7], list_output_filenames[8])

    for fn in list_output_filenames:
        print("plotted: %s" % fn)
