package ca.seibelnet.riemann;

import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.ServerError;
import com.aphyr.riemann.client.MsgTooLargeException;
import com.aphyr.riemann.Proto;
import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * This is basically a direct copy of {{GraphiteReporter}} adapted to use RiemannClient
 */
public class RiemannReporter extends ScheduledReporter {

    /**
     * Returns a new {@link Builder} for {@link RiemannReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link RiemannReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link RiemannReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private Float ttl;
        private String prefix;
        private String separator;
        private String localHost;
        private final List<String> tags;


        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.ttl = null;
            this.tags = new ArrayList<String>();
            this.prefix = null;
            this.separator = " ";
            try {
                this.localHost = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
            }
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param ttl Default time to live of Riemann event.
         * @return {@code this}
         */
        public Builder withTtl(Float ttl) {
            this.ttl = ttl;
            return this;
        }


        public Builder useSeparator(String s) { separator = s; return this; }
        public Builder localHost(String l) { localHost = l; return this; }
        public Builder tags(Collection<String> ts) { tags.clear(); tags.addAll(ts); return this; }


        /**
         * Builds a {@link RiemannReporter} with the given properties, sending metrics using the
         * given {@link Riemann} event.
         *
         * @param riemann a {@link Riemann} event
         * @return a {@link RiemannReporter}
         */
        public RiemannReporter build(Riemann riemann) {
            return new RiemannReporter(registry,
                    riemann,
                    clock,
                    rateUnit,
                    durationUnit,
                    ttl,
                    prefix,
                    separator,
                    localHost,
                    tags,
                    filter);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RiemannReporter.class);

    private final Riemann riemann;
    private final Clock clock;
    private final String prefix;
    private final String separator;
    private final String localHost;
    private final List<String> tags;
    private final Float ttl;

    private RiemannReporter(MetricRegistry registry,
                            Riemann riemann,
                            Clock clock,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            Float ttl,
                            String prefix,
                            String separator,
                            String localHost,
                            List<String> tags,
                            MetricFilter filter) {
        super(registry, "riemann-reporter", filter, rateUnit, durationUnit);
        this.riemann = riemann;
        this.clock = clock;
        this.prefix = prefix;
        this.separator = separator;
        this.localHost = localHost;
        this.tags = tags;
        this.ttl = ttl;
    }

    private EventDSL newEvent(String name, String... components) throws IOException {

        if (!riemann.client.isConnected()) {
            log.error("Client not connected.");
            //TODO: something better -- this kills the reporter.
            throw new IOException("Client not connected.");
        }

        EventDSL event = riemann.client.event();
        if (localHost != null) {
            event.host(localHost);
        }
        if (ttl != null) {
            event.ttl(ttl);
        }
        if (!tags.isEmpty()) {
            event.tags(tags);
        }
        final StringBuilder sb = new StringBuilder();
        if (this.prefix != null) {
            sb.append(this.prefix);
            sb.append(this.separator);
        }
        sb.append(name);

        for (String part : components) {
            sb.append(part);
            sb.append(this.separator);
        }

        event.service(sb.toString());
        return event;
    }


    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = clock.getTime() / 1000;

        log.debug("Reporting metrics: for {} at {}", timestamp, System.currentTimeMillis());
        // oh it'd be lovely to use Java 7 here
        try {
            riemann.connect();
            List<Proto.Event> events = new ArrayList<Proto.Event>();

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                try {
                    events.add(reportGauge(entry.getKey(), entry.getValue(), timestamp));
                } catch (IllegalStateException e) {
                }
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                events.add(reportCounter(entry.getKey(), entry.getValue(), timestamp));
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                events.addAll(reportHistogram(entry.getKey(), entry.getValue(), timestamp));
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                events.addAll(reportMetered(entry.getKey(), entry.getValue(), timestamp));
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                events.addAll(reportTimer(entry.getKey(), entry.getValue(), timestamp));
            }

            log.trace("Sending events to riemann");
            if (riemann.client.sendEventsWithAck(events)) {
                log.debug("Events ack'd by riemann");
                log.debug("Completed at {}", System.currentTimeMillis());
            } else {
                log.error("Riemann did not acknowledge the events we sent!");
            }

        } catch (MsgTooLargeException e) {
            log.error("List of events sent to Riemann was too big", riemann, e);
        } catch (ServerError e) {
            log.error("Error sending events to Riemann", riemann, e);
        } catch (IOException e) {
            log.warn("Unable to report to Riemann", riemann, e);
        }
    }

    private ArrayList<Proto.Event> reportTimer(String name, Timer timer, long timestamp) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();

        ArrayList<Proto.Event> events = new ArrayList<Proto.Event>();

        events.add(newEvent(name, "max").metric(convertDuration(snapshot.getMax())).time(timestamp).build());
        events.add(newEvent(name, "mean").metric(convertDuration(snapshot.getMean())).time(timestamp).build());
        events.add(newEvent(name, "min").metric(convertDuration(snapshot.getMin())).time(timestamp).build());
        events.add(newEvent(name, "stddev").metric(convertDuration(snapshot.getStdDev())).time(timestamp).build());
        events.add(newEvent(name, "p50").metric(convertDuration(snapshot.getMedian())).time(timestamp).build());
        events.add(newEvent(name, "p75").metric(convertDuration(snapshot.get75thPercentile())).time(timestamp).build());
        events.add(newEvent(name, "p95").metric(convertDuration(snapshot.get95thPercentile())).time(timestamp).build());
        events.add(newEvent(name, "p98").metric(convertDuration(snapshot.get98thPercentile())).time(timestamp).build());
        events.add(newEvent(name, "p99").metric(convertDuration(snapshot.get99thPercentile())).time(timestamp).build());
        events.add(newEvent(name, "p999").metric(convertDuration(snapshot.get999thPercentile())).time(timestamp).build());

        reportMetered(name, timer, timestamp);

        return events;
    }

    private ArrayList<Proto.Event> reportMetered(String name, Metered meter, long timestamp) throws IOException {

        ArrayList<Proto.Event> events = new ArrayList<Proto.Event>();

        events.add(newEvent(name, "count").metric(meter.getCount()).time(timestamp).build());
        events.add(newEvent(name, "m1_rate").metric(convertRate(meter.getOneMinuteRate())).time(timestamp).build());
        events.add(newEvent(name, "m5_rate").metric(convertRate(meter.getFiveMinuteRate())).time(timestamp).build());
        events.add(newEvent(name, "m15_rate").metric(convertRate(meter.getFifteenMinuteRate())).time(timestamp).build());
        events.add(newEvent(name, "mean_rate").metric(convertRate(meter.getMeanRate())).time(timestamp).build());

        return events;
    }

    private List<Proto.Event> reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();

        ArrayList<Proto.Event> events = new ArrayList<Proto.Event>();

        events.add(newEvent(name, "count").metric(histogram.getCount()).time(timestamp).build());
        events.add(newEvent(name, "max").metric(snapshot.getMax()).time(timestamp).build());
        events.add(newEvent(name, "mean").metric(snapshot.getMean()).time(timestamp).build());
        events.add(newEvent(name, "min").metric(snapshot.getMin()).time(timestamp).build());
        events.add(newEvent(name, "stddev").metric(snapshot.getStdDev()).time(timestamp).build());
        events.add(newEvent(name, "p50").metric(snapshot.getMedian()).time(timestamp).build());
        events.add(newEvent(name, "p75").metric(snapshot.get75thPercentile()).time(timestamp).build());
        events.add(newEvent(name, "p95").metric(snapshot.get95thPercentile()).time(timestamp).build());
        events.add(newEvent(name, "p98").metric(snapshot.get98thPercentile()).time(timestamp).build());
        events.add(newEvent(name, "p99").metric(snapshot.get99thPercentile()).time(timestamp).build());
        events.add(newEvent(name, "p999").metric(snapshot.get999thPercentile()).time(timestamp).build());

        return events;

    }

    private Proto.Event reportCounter(String name, Counter counter, long timestamp) throws IOException {
        return newEvent(name, "count").metric(counter.getCount()).time(timestamp).build();
    }

    private Proto.Event reportGauge(String name, Gauge gauge, long timestamp) throws IOException, IllegalStateException {
        Object o = gauge.getValue();

        if (o instanceof Float) {
            return newEvent(name).metric((Float) o).time(timestamp).build();
        } else if (o instanceof Double) {
            return newEvent(name).metric((Double) o).time(timestamp).build();
        } else if (o instanceof Byte) {
            return newEvent(name).metric((Byte) o).time(timestamp).build();
        } else if (o instanceof Short) {
            return newEvent(name).metric((Short) o).time(timestamp).build();
        } else if (o instanceof Integer) {
            return newEvent(name).metric((Integer) o).time(timestamp).build();
        } else if (o instanceof Long) {
            return newEvent(name).metric((Long) o).time(timestamp).build();
        } else {
            log.warn("Gauge was of an unknown type: {}", o.getClass().toString());
            throw new IllegalStateException("Guage was of an unknown type: {}", o.getClass().toString());
            //return null;
        }

    }



}
