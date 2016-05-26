var motApp = angular.module('motApp', ['ngRoute']);

function createCdf(data) {
    var values = data.map(function(d) { return d.count; })
    var sum = d3.sum(values);
    var running = 0
    data.forEach(function(d) {
        running += d.count / sum;
        d.cdf = Math.min(1, running);
        return d
    })
    return data
}

function toTitleCase(str) {
    return str.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
}

function buildTree(data, nameField, depth = 1) {
    var sum = 0;
    data.forEach(function(x) { sum = sum + x.count; });
    var cutOff = sum / 50; // 2%
    var others = data.length > 6 ? data.filter(function(x) { return x.count < cutOff; }) : [];
    var dataToPlot = data.filter(function(x) { return x.count >= cutOff; })

    var othersSum = 0;
    others.forEach(function(x) { othersSum = othersSum + x.count; });
    if(othersSum > 0) {
        var other = { "count": othersSum, "barColour": "#2b8cbe" };
        other[nameField] = "Other " + Math.round((othersSum * 100) / sum) + "%";
        other.children = (depth <= 4) ? buildTree(others, nameField, depth + 1).children : []
        dataToPlot.push(other);
    }

    dataToPlot = dataToPlot.map(function(d) {
        d[nameField] = toTitleCase(d[nameField])
        if(d.children) {
            d.children = buildTree(d.children, nameField, depth + 1).children
        }
        return d
    })

    var x = {}
    x[nameField] = "All Data";
    x.children = dataToPlot;
    x.count = sum;
    return x;
}

motApp
	.controller('MotController', function($scope, $http) {

        $http.get("results/passRateByAgeBand.json").success(function(data) {
            var sorted = data.sort(function(a, b) { return a.age - b.age; })
            $scope.passRate100 = sorted
            $scope.passRate20 = sorted.slice(0, 21)
            var count = 0
            sorted.forEach(function (row) { count += row.count; })
            $scope.recordCount = count.toLocaleString()
            cdf = createCdf(sorted)
            $scope.cdf100 = cdf
            $scope.cdf20 = cdf.slice(0, 21)
        });

        var colourMap = {"silver": "#DDD",
                         "blue": "#1f77b4",
                         "red": "#d62728",
                         "black": "#333",
                         "green": "#2ca02c",
                         "grey": "#AAA",
                         "white": "#EEE",
                         "gold": "#e7ba52",
                         "yellow": "yellow",
                         "beige": "#FFFACD",
                         "purple": "#9467bd",
                         "orange": "#ff7f0e",
                         "turquoise": "#17becf",
                         "bronze": "#cd7f32",
                         "brown": "#8b4513",
                         "cream": "#FFFACD",
                         "multi-colour": "#1f77b4",
                         "pink": "#de9ed6",
                         "not stated": "#1f77b4"
                         }

        $http.get("results/motTestsByVehicleColour.json").success(function(data) {
            var colours = data.sort(function(a, b) { return b.count - a.count })
            $scope.colours = colours.map(function(x) {
                x.barColour = colourMap[x.colour];
                return x;
            })

            $scope.colourTreeData = buildTree(data, "colour")
        });

        $http.get("results/motTestsByMake.json").success(function(data) {
            $scope.makeTreeData = buildTree(data, "make")
        });

        $http.get("results/motTestsByMakeAndModel.json").success(function(data) {
            $scope.makeAndModelTreeData = buildTree(data, "name")
        });

        $scope.formatRate = d3.format(".1f")
        $http.get("results/passRateByMake.json").success(function(data) {
            $scope.passRateByMake = data.map(function(d) { d.make = toTitleCase(d.make); return d; })
            $scope.makeCount = data.length
            $scope.searchMake = function(item){
                if (!$scope.makeFilter || (item.make.toLowerCase().indexOf($scope.makeFilter.toLowerCase()) != -1)) {
                    return true;
                }
                return false;
             };
        });

        $scope.formatRate = d3.format(".1f")
        $http.get("results/passRateByMakeAndModel.json").success(function(data) {
            $scope.passRateByMakeAndModel = data.map(function(d) { d.model = toTitleCase(d.model); d.make = toTitleCase(d.make); return d; })
            $scope.makeAndModelCount = data.length
            $scope.searchMakeAndModel = function(item){
                if (!$scope.makeAndModelFilter || (item.make.toLowerCase().indexOf($scope.makeAndModelFilter.toLowerCase()) != -1)
                                               || (item.model.toLowerCase().indexOf($scope.makeAndModelFilter.toLowerCase()) != -1)) {
                    return true;
                }
                return false;
             };
        });

	})
	.directive("treeMapChart", treeMapChartDirective)
	.directive("categoryBarChart", categoryBarChartDirective)
	.directive("lineChart", lineChartDirective)
	.directive("columnChart", columnChartDirective)