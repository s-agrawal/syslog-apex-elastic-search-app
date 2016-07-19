package com.synerzip.apexelasticsearchapp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Saurabh Agrawal <saurabh.agrawal@synerzip.com> on 22/6/16.
 */
public class RequestFilterAggregator extends BaseOperator {

    Map<String, RequestCount> bu_wise_request_filter_count = new HashMap<String, RequestCount>();

    public final transient DefaultOutputPort<Map<String, Object>> outputPortElastic = new DefaultOutputPort();

    public final transient DefaultOutputPort<Map<String, Object>> outputPortConsole = new DefaultOutputPort();

    public final transient DefaultInputPort<byte[]> inputPort = new DefaultInputPort<byte[]>() {

        @Override
        public void process(byte[] tuple) {
            String message = new String(tuple);
            String bu = getBU(message);

            if (bu != "") {
                String filter = getFilter(message);
                if (filter != "") {
                    updateRequestCount(bu, filter);
                }
            }
        }
    };

    /**
     * Extract BU name from message
     * @param message syslog message
     * @return BU name if found else empty string
     */
    private String getBU(String message) {
        Pattern bu_string_pattern = Pattern.compile("[(][a-zA-Z[-]]+[-]TCP[-]UDP[-]proxy[-]00[)]");
        Matcher bu_string_matcher = bu_string_pattern.matcher(message);
        if (bu_string_matcher.find()) {
            String bu_string = bu_string_matcher.group();
            Pattern bu_name_pattern = Pattern.compile("[(](.*?)[-]TCP[-]UDP[-]proxy[-]00[)]");
            Matcher bu_name_matcher = bu_name_pattern.matcher(bu_string);
            bu_name_matcher.find();
            ArrayList<String> bu_components = new ArrayList<String>();
            for(String word: bu_name_matcher.group(1).split("-")) {
                bu_components.add(WordUtils.capitalizeFully(word.toLowerCase()));
            }
            String bu = StringUtils.join(bu_components, "");
            return bu;
        } else {
            return "";
        }
    }

    private String getFilter(String message) {
        String filter = "";
        if (message.matches(".*Allow.*")) {
            filter = "Allow";
        } else if (message.matches(".*Deny.*")) {
            filter = "Deny";
        }
        return filter;
    }

    private void updateRequestCount(String bu, String filter) {
        if (!bu_wise_request_filter_count.containsKey(bu)) {
            bu_wise_request_filter_count.put(bu, new RequestCount());
        }

        RequestCount bu_request_count = bu_wise_request_filter_count.get(bu);

        int allowedRequests = bu_request_count.getAllowedRequestCount();
        int deniedRequests = bu_request_count.getDeniedRequestCount();

        if (filter == "Allow") {
            allowedRequests = allowedRequests + 1;
        } else if(filter == "Deny") {
            deniedRequests = deniedRequests + 1;
        }

        RequestCount updated_bu_request_count = new RequestCount(allowedRequests, deniedRequests);
        bu_wise_request_filter_count.put(bu, updated_bu_request_count);
    }

    @Override
    public void beginWindow(long windowId) {
        bu_wise_request_filter_count = new HashMap<String, RequestCount>();
    }

    @Override
    public void endWindow() {
        if (!bu_wise_request_filter_count.isEmpty()) {
            for (String bu: bu_wise_request_filter_count.keySet()) {
                Map<String, Object> output = new HashMap<String, Object>();
                String randomId = UUID.randomUUID().toString();
                output.put("id", randomId);
                output.put("bu", bu);
                int allowedRequests = bu_wise_request_filter_count.get(bu).getAllowedRequestCount();
                output.put("allowed_requests", allowedRequests);
                int deniedRequests = bu_wise_request_filter_count.get(bu).getDeniedRequestCount();
                output.put("denied_requests", deniedRequests);
                outputPortElastic.emit(output);
            }
        }
    }
}