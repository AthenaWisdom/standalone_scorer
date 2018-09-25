import numpy as np
import re
from math import sqrt


class ReportProbabilityInterpolation(object):
    def calculate(self, hit_rates_artifact, present_num_scored_candidates, past_num_candidates, past_scored_count, epsilon=0.0001):

        hit_rate, past_percentiles = self.__get_hit_rate_and_percentiles_arrays(hit_rates_artifact, past_num_candidates, past_scored_count)

        percentiles_to_hit_rate = np.zeros([past_percentiles.shape[0], 2])
        percentiles_to_hit_rate[:, 0] = past_percentiles
        percentiles_to_hit_rate[:, 1] = hit_rate
        percentiles_to_hit_rate.view('i8,i8').sort(order=['f0'], axis=0)
        sorted_past_percentiles = percentiles_to_hit_rate[:, 0]
        sorted_by_percentiles_hit_rates = percentiles_to_hit_rate[:, 1]

        y = np.zeros(sorted_past_percentiles.shape[0])
        y[0] = sorted_by_percentiles_hit_rates[0]
        for n in range(1, sorted_past_percentiles.shape[0]):
            y[n] = (sorted_by_percentiles_hit_rates[n] * sorted_past_percentiles[n] -
                    sorted_by_percentiles_hit_rates[n - 1] * sorted_past_percentiles[n - 1]) / \
                   (sorted_past_percentiles[n] - sorted_past_percentiles[n - 1])  # calculate the precent for each segment
        y = np.clip(y, 0.0, np.max(sorted_by_percentiles_hit_rates))
        present_percentiles = np.arange(0, present_num_scored_candidates) / float(
            present_num_scored_candidates)  # this is the range of present candidates
        probability_vector = np.clip(np.sort(np.interp(present_percentiles, sorted_past_percentiles, y))[::-1], 0.0,
                                     1.0)  # interpolate and sort

        points = zip(present_percentiles, probability_vector)
        significant_points = self.__rdp(points, epsilon)
        compressed_graph_x = [x for x, y in significant_points]
        compressed_graph_y = [y for x, y in significant_points]

        # final_probability_vector = inteoplate again ???

        return probability_vector, {'compressed_graph_x': compressed_graph_x, 'compressed_graph_y': compressed_graph_y}

    def __best_effort_get_percentile(self, key, num_candidates, num_scored):
        match = re.match(r'hr_count([0-9]+)', key)
        if match:
            count = int(match.group(1))
            if count < num_scored:
                return count / float(num_scored)
            return None

        match = re.match(r'hr_prcntg([0-9]+)', key)
        if match:
            old_prcntg = int(match.group(1)) / 100.0
            rank = old_prcntg * num_candidates
            if rank <= num_scored:
                return rank / float(num_scored)

        return None

    def __get_hit_rate_and_percentiles_arrays(self, past_hit_rates, past_num_candidates, past_num_scored):
        past_percentiles = []
        hit_rate = []

        for key in past_hit_rates.keys():
            percentile = self.__best_effort_get_percentile(key, past_num_candidates, past_num_scored)

            if percentile is not None:
                hit_rate.append(past_hit_rates[key])
                past_percentiles.append(percentile)

        return np.array(hit_rate), np.array(past_percentiles)

    def __distance(self, a, b):
        return sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

    def __point_line_distance(self, point, start, end):
        if start == end:
            return self.__distance(point, start)
        else:
            n = abs(
                (end[0] - start[0]) * (start[1] - point[1]) - (start[0] - point[0]) * (end[1] - start[1])
            )
            d = sqrt(
                (end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2
            )
            return n / d

    # copied from https://github.com/sebleier/RDP/blob/master/__init__.py
    def __rdp(self, points, epsilon):
        """
        Reduces a series of points to a simplified version that loses detail, but
        maintains the general shape of the series.
        """
        dmax = 0.0
        index = 0
        for i in range(1, len(points) - 1):
            d = self.__point_line_distance(points[i], points[0], points[-1])
            if d > dmax:
                index = i
                dmax = d
        if dmax >= epsilon:
            results = self.__rdp(points[:index + 1], epsilon)[:-1] + self.__rdp(points[index:], epsilon)
        else:
            results = [points[0], points[-1]]
        return results
