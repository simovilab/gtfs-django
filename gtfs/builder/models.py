from django.db import models

from gtfs import fields as gtfs_fields

# -------------
# GTFS Schedule
# --------------


class BaseAgency(models.Model):
    """Transit agencies with service represented in this dataset.

    Maps to agency.txt.
    This model does not include primary keys."""

    agency_id = gtfs_fields.GTFSIDField(
        help_text='Identifies a transit brand which is often synonymous with a transit agency. Note that in some cases, such as when a single agency operates multiple separate services, agencies and brands are distinct. This document uses the term "agency" in place of "brand". A dataset may contain data from multiple agencies.',
        blank=True,
        null=True,
    )
    agency_name = gtfs_fields.GTFSTextField(
        help_text="Full name of the transit agency.",
    )
    agency_url = gtfs_fields.GTFSURLField(
        help_text="URL of the transit agency.",
    )
    agency_timezone = gtfs_fields.GTFSTimezoneField(
        help_text="Timezone where the transit agency is located. If multiple agencies are specified in the dataset, each must have the same agency_timezone.",
    )
    agency_lang = gtfs_fields.GTFSLanguageCodeField(
        help_text="Primary language used by this transit agency. Should be provided to help GTFS consumers choose capitalization rules and other language-specific settings for the dataset.",
        blank=True,
        null=True,
    )
    agency_phone = gtfs_fields.GTFSPhoneNumberField(
        help_text="A voice telephone number for the specified agency. This field is a string value that presents the telephone number as typical for the agency's service area. It may contain punctuation marks to group the digits of the number. Dialable text (for example, TriMet's \"503-238-RIDE\") is permitted, but the field must not contain any other descriptive text.",
        blank=True,
        null=True,
    )
    agency_fare_url = gtfs_fields.GTFSURLField(
        help_text="URL of a web page where a rider can purchase tickets or other fare instruments for that agency, or a web page containing information about that agency's fares.",
        blank=True,
        null=True,
    )
    agency_email = gtfs_fields.GTFSEmailField(
        help_text="Email address actively monitored by the agency's customer service department. This email address should be a direct contact point where transit riders can reach a customer service representative at the agency.",
        blank=True,
        null=True,
    )
    cemv_support = gtfs_fields.GTFSEnumField(
        help_text="Indicates if riders can access a transit service (i.e., trip) associated with this agency by using a contactless EMV (Europay, Mastercard, and Visa) card or mobile device as fare media at a fare validator (such as in pay-as-you-go or open-loop systems). This field does not indicate that cEMV can be used to purchase other fare products or to add value to another fare media. Support for cEMVs should only be indicated if all services under this agency are accessible with the use of cEMV cards or mobile devices as fare media. If both agency.cemv_support and routes.cemv_support are provided for the same service, the value in routes.cemv_support shall take precedence. This field is independent of all other fare-related files and may be used separately. If there is conflicting information between this field and any fare-related file (such as fare_media.txt, fare_products.txt, or fare_leg_rules.txt), the information in those files shall take precedence over agency.cemv_support.",
        blank=True,
        null=True,
        choices=(
            (0, "No cEMV information for trips associated with this agency."),
            (
                1,
                "Riders may use cEMVs as fare media for trips associated with this agency.",
            ),
            (
                2,
                "cEMVs are not supported as fare media for trips associated with this agency.",
            ),
        ),
    )

    class Meta:
        abstract = True


class BaseStop(models.Model):
    """Stops where vehicles pick up or drop off riders. Also defines stations and station entrances.

    Maps to stops.txt.
    This model does not include primary keys."""

    stop_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a location: stop/platform, station, entrance/exit, generic node or boarding area (see location_type).",
    )
    stop_code = gtfs_fields.GTFSTextField(
        help_text="Short text or a number that identifies the location for riders. These codes are often used in phone-based transit information systems or printed on signage to make it easier for riders to get information for a particular location. The stop_code may be the same as stop_id if it is public facing. This field should be left empty for locations without a code presented to riders.",
        blank=True,
        null=True,
    )
    stop_name = gtfs_fields.GTFSTextField(
        help_text='Name of the location. The stop_name should match the agency\'s rider-facing name for the location as printed on a timetable, published online, or represented on signage. For translations into other languages, use translations.txt. When the location is a boarding area (location_type=4), the stop_name should contains the name of the boarding area as displayed by the agency. It could be just one letter (like on some European intercity railway stations), or text like "Wheelchair boarding area" (NYC\'s Subway) or "Head of short trains" (Paris\' RER).',
        blank=True,
        null=True,
    )
    tts_stop_name = gtfs_fields.GTFSTextField(
        help_text='Readable version of the stop_name. See "Text-to-speech field" in the Term Definitions for more.',
        blank=True,
        null=True,
    )
    stop_desc = gtfs_fields.GTFSTextField(
        help_text="Description of the location that provides useful, quality information. Should not be a duplicate of stop_name.",
        blank=True,
        null=True,
    )
    stop_lat = gtfs_fields.GTFSLatitudeField(
        help_text="Latitude of the location. For stops/platforms (location_type=0) and boarding area (location_type=4), the coordinates must be the ones of the bus pole — if exists — and otherwise of where the travelers are boarding the vehicle (on the sidewalk or the platform, and not on the roadway or the track where the vehicle stops).",
        blank=True,
        null=True,
    )
    stop_lon = gtfs_fields.GTFSLongitudeField(
        help_text="Longitude of the location. For stops/platforms (location_type=0) and boarding area (location_type=4), the coordinates must be the ones of the bus pole — if exists — and otherwise of where the travelers are boarding the vehicle (on the sidewalk or the platform, and not on the roadway or the track where the vehicle stops).",
        blank=True,
        null=True,
    )
    zone_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the fare zone for a stop. If this record represents a station or station entrance, the zone_id is ignored.",
        blank=True,
        null=True,
    )
    stop_url = gtfs_fields.GTFSURLField(
        help_text="URL of a web page about the location. This should be different from the agency.agency_url and the routes.route_url field values.",
        blank=True,
        null=True,
    )
    location_type = gtfs_fields.GTFSEnumField(
        help_text="Location type.",
        blank=True,
        null=True,
        choices=(
            (
                0,
                "Stop (or Platform). A location where passengers board or disembark from a transit vehicle. Is called a platform when defined within a parent_station.",
            ),
            (
                1,
                "Station. A physical structure or area that contains one or more platform.",
            ),
            (
                2,
                "Entrance/Exit. A location where passengers can enter or exit a station from the street. If an entrance/exit belongs to multiple stations, it may be linked by pathways to both, but the data provider must pick one of them as parent.",
            ),
            (
                3,
                "Generic Node. A location within a station, not matching any other location_type, that may be used to link together pathways define in pathways.txt.",
            ),
            (
                4,
                "Boarding Area. A specific location on a platform, where passengers can board and/or alight vehicles.",
            ),
        ),
    )
    parent_station = gtfs_fields.GTFSIDField(
        help_text="Defines hierarchy between the different locations defined in stops.txt. It contains the ID of the parent location, as followed: Stop/platform (location_type=0): the parent_station field contains the ID of a station. Station (location_type=1): this field must be empty. Entrance/exit (location_type=2) or generic node (location_type=3): the parent_station field contains the ID of a station (location_type=1). Boarding Area (location_type=4): the parent_station field contains ID of a platform.",
        blank=True,
        null=True,
    )
    stop_timezone = gtfs_fields.GTFSTimezoneField(
        help_text="Timezone of the location. If the location has a parent station, it inherits the parent station's timezone instead of applying its own. Stations and parentless stops with empty stop_timezone inherit the timezone specified by agency.agency_timezone. The times provided in stop_times.txt are in the timezone specified by agency.agency_timezone, not stop_timezone. This ensures that the time values in a trip always increase over the course of a trip, regardless of which timezones the trip crosses.",
        blank=True,
        null=True,
    )
    wheelchair_boarding = gtfs_fields.GTFSEnumField(
        help_text="Indicates whether wheelchair boardings are possible from the location.",
        blank=True,
        null=True,
        choices=(
            (
                "For parentless stops",
                (
                    (0, "No accessibility information for the stop."),
                    (
                        1,
                        "Some vehicles at this stop can be boarded by a rider in a wheelchair.",
                    ),
                    (2, "Wheelchair boarding is not possible at this stop."),
                ),
            ),
            (
                "For child stops",
                (
                    (
                        0,
                        "Stop will inherit its wheelchair_boarding behavior from the parent station, if specified in the parent.",
                    ),
                    (
                        1,
                        "There exists some accessible path from outside the station to the specific stop/platform.",
                    ),
                    (
                        2,
                        "There exists no accessible path from outside the station to the specific stop/platform.",
                    ),
                ),
            ),
            (
                "For station entrances/exits",
                (
                    (
                        0,
                        "Station entrance will inherit its wheelchair_boarding behavior from the parent station, if specified for the parent.",
                    ),
                    (1, "Station entrance is wheelchair accessible."),
                    (2, "No accessible path from station entrance to stops/platforms."),
                ),
            ),
        ),
    )
    level_id = gtfs_fields.GTFSIDField(
        help_text="Level of the location. The same level may be used by multiple unlinked stations.",
        blank=True,
        null=True,
    )
    platform_code = gtfs_fields.GTFSTextField(
        help_text='Platform identifier for a platform stop (a stop belonging to a station). This should be just the platform identifier (eg. "G" or "3"). Words like "platform" or "track" (or the feed\'s language-specific equivalent) should not be included. This allows feed consumers to more easily internationalize and localize the platform identifier into other languages.',
        blank=True,
        null=True,
    )
    stop_access = gtfs_fields.GTFSEnumField(
        help_text="Indicates how the stop is accessed for a particular station. When stop_access is empty, the access for the specified stop or platform is considered undefined.",
        blank=True,
        null=True,
        choices=(
            (
                0,
                "The stop/platform cannot be directly accessed from the street network. It must be accessed from a station entrance if there is one defined for the station, otherwise the station itself. If there are pathways defined for the station, they must be used to access the stop/platform.",
            ),
            (
                1,
                "Consuming applications should generate directions for access directly to the stop, independent of any entrances or pathways of the parent station.",
            ),
        ),
    )

    class Meta:
        abstract = True


class BaseRoute(models.Model):
    """Transit routes. A route is a group of trips that are displayed to riders as a single service.

    Maps to routes.txt.
    This model does not include primary keys."""

    route_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a route.",
    )
    agency_id = gtfs_fields.GTFSIDField(
        help_text="Agency for the specified route.",
        blank=True,
        null=True,
    )
    route_short_name = gtfs_fields.GTFSTextField(
        help_text='Short name of a route. Often a short, abstract identifier (e.g., "32", "100X", "Green") that riders use to identify a route. Both route_short_name and route_long_name may be defined.',
        blank=True,
        null=True,
    )
    route_long_name = gtfs_fields.GTFSTextField(
        help_text="Full name of a route. This name is generally more descriptive than the route_short_name and often includes the route's destination or stop. Both route_short_name and route_long_name may be defined.",
        blank=True,
        null=True,
    )
    route_desc = gtfs_fields.GTFSTextField(
        help_text="Description of a route that provides useful, quality information. Should not be a duplicate of route_short_name or route_long_name.",
        blank=True,
        null=True,
    )
    route_type = gtfs_fields.GTFSEnumField(
        help_text="Indicates the type of transportation used on a route.",
        choices=(
            (
                0,
                "Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area.",
            ),
            (
                1,
                "Subway, Metro. Any underground rail system within a metropolitan area.",
            ),
            (2, "Rail. Used for intercity or long-distance travel."),
            (3, "Bus. Used for short- and long-distance bus routes."),
            (4, "Ferry. Used for short- and long-distance boat service."),
            (
                5,
                "Cable tram. Used for street-level rail cars where the cable runs beneath the vehicle (e.g., cable car in San Francisco).",
            ),
            (
                6,
                "Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables.",
            ),
            (7, "Funicular. Any rail system designed for steep inclines."),
            (
                11,
                "Trolleybus. Electric buses that draw power from overhead wires using poles.",
            ),
            (
                12,
                "Monorail. Railway in which the track consists of a single rail or a beam.",
            ),
        ),
    )
    route_url = gtfs_fields.GTFSURLField(
        help_text="URL of a web page about the particular route. Should be different from the agency.agency_url value.",
        blank=True,
        null=True,
    )
    route_color = gtfs_fields.GTFSColorField(
        help_text="Route color designation that matches public facing material. Defaults to white (FFFFFF) when omitted or left empty. The color difference between route_color and route_text_color should provide sufficient contrast when viewed on a black and white screen.",
        blank=True,
        null=True,
    )
    route_text_color = gtfs_fields.GTFSColorField(
        help_text="Legible color to use for text drawn against a background of route_color. Defaults to black (000000) when omitted or left empty. The color difference between route_color and route_text_color should provide sufficient contrast when viewed on a black and white screen.",
        blank=True,
        null=True,
    )
    route_sort_order = gtfs_fields.GTFSNonNegativeIntegerField(
        help_text="Orders the routes in a way which is ideal for presentation to customers. Routes with smaller route_sort_order values should be displayed first.",
        blank=True,
        null=True,
    )
    continuous_pickup = gtfs_fields.GTFSEnumField(
        help_text="Indicates that the rider can board the transit vehicle at any point along the vehicle's travel path as described by shapes.txt, on every trip of the route. Values for routes.continuous_pickup may be overridden by defining values in stop_times.continuous_pickup for specific stop_times along the route.",
        blank=True,
        null=True,
        choices=(
            (0, "Continuous stopping pickup."),
            (1, "No continuous stopping pickup."),
            (2, "Must phone agency to arrange continuous stopping pickup."),
            (3, "Must coordinate with driver to arrange continuous stopping pickup."),
        ),
    )
    continuous_drop_off = gtfs_fields.GTFSEnumField(
        help_text="Indicates that the rider can alight from the transit vehicle at any point along the vehicle's travel path as described by shapes.txt, on every trip of the route. Values for routes.continuous_drop_off may be overridden by defining values in stop_times.continuous_drop_off for specific stop_times along the route.",
        blank=True,
        null=True,
        choices=(
            (0, "Continuous stopping drop off."),
            (1, "No continuous stopping drop off."),
            (2, "Must phone agency to arrange continuous stopping drop off."),
            (3, "Must coordinate with driver to arrange continuous stopping drop off."),
        ),
    )
    network_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a group of routes. Multiple rows in routes.txt may have the same network_id.",
        blank=True,
        null=True,
    )
    cemv_support = gtfs_fields.GTFSEnumField(
        help_text="Indicates if riders can access a transit service (i.e., trip) associated with this route by using a contactless EMV (Europay, Mastercard, and Visa) card or mobile device as fare media at a fare validator (such as in pay-as-you-go or open-loop systems). This field does not indicate that cEMV can be used to purchase other fare products or to add value to another fare media. Support for cEMVs should only be indicated if all services under this route are accessible with the use of cEMV cards or mobile devices as fare media. If both agency.cemv_support and routes.cemv_support are provided for the same service, the value in routes.cemv_support shall take precedence. This field is independent of all other fare-related files and may be used separately. If there is conflicting information between this field and any fare-related file (such as fare_media.txt, fare_products.txt, or fare_leg_rules.txt), the information in those files shall take precedence over agency.cemv_support.",
        blank=True,
        null=True,
        choices=(
            (0, "No cEMV information for trips associated with this route."),
            (
                1,
                "Riders may use cEMVs as fare media for trips associated with this route.",
            ),
            (
                2,
                "cEMVs are not supported as fare media for trips associated with this route.",
            ),
        ),
    )

    class Meta:
        abstract = True


class BaseShape(models.Model):
    """Rules for mapping vehicle travel paths, sometimes referred to as route alignments.

    Maps to shapes.txt.
    This model does not include primary keys."""

    shape_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a shape.",
    )
    shape_pt_lat = gtfs_fields.GTFSLatitudeField(
        help_text="Latitude of a shape point. Each record in shapes.txt represents a shape point used to define the shape.",
    )
    shape_pt_lon = gtfs_fields.GTFSLongitudeField(
        help_text="Longitude of a shape point.",
    )
    shape_pt_sequence = gtfs_fields.GTFSNonNegativeIntegerField(
        help_text="Sequence in which the shape points connect to form the shape. Values must increase along the trip but do not need to be consecutive.",
    )
    shape_dist_traveled = gtfs_fields.GTFSNonNegativeFloatField(
        help_text="Actual distance traveled along the shape from the first shape point to the point specified in this record. Used by trip planners to show the correct portion of the shape on a map. Values must increase along with shape_pt_sequence; they must not be used to show reverse travel along a route. Distance units must be consistent with those used in stop_times.txt.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseCalendar(models.Model):
    """Service dates specified using a weekly schedule with start and end dates.

    Maps to calendar.txt.
    This model does not include primary keys."""

    service_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a set of dates when service is available for one or more routes.",
    )
    monday = gtfs_fields.GTFSEnumField(
        help_text="Indicates whether the service operates on all Mondays in the date range specified by the start_date and end_date fields. Note that exceptions for particular dates may be listed in calendar_dates.txt.",
        choices=(
            (1, "Service is available for all Mondays in the date range."),
            (0, "Service is not available for Mondays in the date range."),
        ),
    )
    tuesday = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as monday except applies to Tuesdays.",
        choices=(
            (1, "Service is available for all Tuesdays in the date range."),
            (0, "Service is not available for Tuesdays in the date range."),
        ),
    )
    wednesday = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as monday except applies to Wednesdays.",
        choices=(
            (1, "Service is available for all Wednesdays in the date range."),
            (0, "Service is not available for Wednesdays in the date range."),
        ),
    )
    thursday = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as monday except applies to Thursdays.",
        choices=(
            (1, "Service is available for all Thursdays in the date range."),
            (0, "Service is not available for Thursdays in the date range."),
        ),
    )
    friday = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as monday except applies to Fridays.",
        choices=(
            (1, "Service is available for all Fridays in the date range."),
            (0, "Service is not available for Fridays in the date range."),
        ),
    )
    saturday = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as monday except applies to Saturdays.",
        choices=(
            (1, "Service is available for all Saturdays in the date range."),
            (0, "Service is not available for Saturdays in the date range."),
        ),
    )
    sunday = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as monday except applies to Sundays.",
        choices=(
            (1, "Service is available for all Sundays in the date range."),
            (0, "Service is not available for Sundays in the date range."),
        ),
    )
    start_date = gtfs_fields.GTFSDateField(
        help_text="Start service day for the service interval.",
    )
    end_date = gtfs_fields.GTFSDateField(
        help_text="End service day for the service interval. This service day is included in the interval.",
    )

    class Meta:
        abstract = True


class BaseTrip(models.Model):
    """Trips for each route. A trip is a sequence of two or more stops that occur during a specific time period.

    Maps to trips.txt.
    This model does not include primary keys."""

    route_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a route.",
    )
    service_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a set of dates when service is available for one or more routes.",
    )
    trip_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a trip.",
    )
    trip_headsign = gtfs_fields.GTFSTextField(
        help_text="Text that appears on signage identifying the trip's destination to riders. This field is recommended for all services with headsign text displayed on the vehicle which may be used to distinguish amongst trips in a route.",
        blank=True,
        null=True,
    )
    trip_short_name = gtfs_fields.GTFSTextField(
        help_text="Public-facing text used to identify the trip to riders, for instance, to identify train numbers for commuter rail trips. If riders do not commonly rely on trip names, trip_short_name should be empty.",
        blank=True,
        null=True,
    )
    direction_id = gtfs_fields.GTFSEnumField(
        help_text="Indicates the direction of travel for a trip. This field should not be used in routing; it provides a way to separate trips by direction when publishing time tables.",
        blank=True,
        null=True,
        choices=(
            (0, "Travel in one direction (e.g., outbound travel)."),
            (1, "Travel in the opposite direction (e.g., inbound travel)."),
        ),
    )
    block_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the block to which the trip belongs. A block consists of a single trip or many sequential trips made using the same vehicle, defined by shared service days and block_id. A block_id may have trips with different service days, making distinct blocks.",
        blank=True,
        null=True,
    )
    shape_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a geospatial shape describing the vehicle travel path for a trip.",
        blank=True,
        null=True,
    )
    wheelchair_accessible = gtfs_fields.GTFSEnumField(
        help_text="Indicates wheelchair accessibility.",
        blank=True,
        null=True,
        choices=(
            (0, "No accessibility information for the trip."),
            (
                1,
                "Vehicle being used on this particular trip can accommodate at least one rider in a wheelchair.",
            ),
            (2, "No riders in wheelchairs can be accommodated on this trip."),
        ),
    )
    bikes_allowed = gtfs_fields.GTFSEnumField(
        help_text="Indicates whether bikes are allowed.",
        blank=True,
        null=True,
        choices=(
            (0, "No bike information for the trip."),
            (
                1,
                "Vehicle being used on this particular trip can accommodate at least one bicycle.",
            ),
            (2, "No bicycles are allowed on this trip."),
        ),
    )
    cars_allowed = gtfs_fields.GTFSEnumField(
        help_text="Indicates whether cars are allowed.",
        blank=True,
        null=True,
        choices=(
            (0, "No car information for the trip."),
            (
                1,
                "Vehicle being used on this particular trip can accommodate at least one car.",
            ),
            (2, "No cars are allowed on this trip."),
        ),
    )
    safe_duration_factor = gtfs_fields.GTFSFloatField(
        help_text="Multiplier applied to travel time estimates calculated for on-demand trips.",
        blank=True,
        null=True,
    )
    safe_duration_offset = gtfs_fields.GTFSFloatField(
        help_text="Fixed offset value in seconds applied to travel time estimates calculated for on-demand trips.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseStopTime(models.Model):
    """Times that a vehicle arrives at and departs from stops for each trip.

    Maps to stop_times.txt.
    This model does not include primary keys."""

    trip_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a trip.",
    )
    arrival_time = gtfs_fields.GTFSTimeField(
        help_text="Arrival time at the stop (defined by stop_times.stop_id) for a specific trip (defined by stop_times.trip_id) in the time zone specified by agency.agency_timezone, not stops.stop_timezone.",
        blank=True,
        null=True,
    )
    departure_time = gtfs_fields.GTFSTimeField(
        help_text="Departure time from the stop (defined by stop_times.stop_id) for a specific trip (defined by stop_times.trip_id) in the time zone specified by agency.agency_timezone, not stops.stop_timezone.",
        blank=True,
        null=True,
    )
    stop_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the serviced stop.",
        blank=True,
        null=True,
    )
    location_group_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the serviced location group that indicates groups of stops where riders may request pickup or drop off.",
        blank=True,
        null=True,
    )
    location_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the GeoJSON location that corresponds to serviced zone where riders may request pickup or drop off.",
        blank=True,
        null=True,
    )
    stop_sequence = gtfs_fields.GTFSNonNegativeIntegerField(
        help_text="Order of stops, location groups, or GeoJSON locations for a particular trip. The values must increase along the trip but do not need to be consecutive.",
    )
    stop_headsign = gtfs_fields.GTFSTextField(
        help_text="Text that appears on signage identifying the trip's destination to riders. This field overrides the default trips.trip_headsign when the headsign changes between stops. If the headsign is displayed for an entire trip, trips.trip_headsign should be used instead.",
        blank=True,
        null=True,
    )
    start_pickup_drop_off_window = gtfs_fields.GTFSTimeField(
        help_text="Time that on-demand service becomes available in a GeoJSON location, location group, or stop.",
        blank=True,
        null=True,
    )
    end_pickup_drop_off_window = gtfs_fields.GTFSTimeField(
        help_text="Time that on-demand service ends in a GeoJSON location, location group, or stop.",
        blank=True,
        null=True,
    )
    pickup_type = gtfs_fields.GTFSEnumField(
        help_text="Indicates pickup method.",
        blank=True,
        null=True,
        choices=(
            (0, "Regularly scheduled pickup."),
            (1, "No pickup available."),
            (2, "Must phone agency to arrange pickup."),
            (3, "Must coordinate with driver to arrange pickup."),
        ),
    )
    drop_off_type = gtfs_fields.GTFSEnumField(
        help_text="Indicates drop off method.",
        blank=True,
        null=True,
        choices=(
            (0, "Regularly scheduled drop off."),
            (1, "No drop off available."),
            (2, "Must phone agency to arrange drop off."),
            (3, "Must coordinate with driver to arrange drop off."),
        ),
    )
    continuous_pickup = gtfs_fields.GTFSEnumField(
        help_text="Indicates that the rider can board the transit vehicle at any point along the vehicle’s travel path as described by shapes.txt, from this stop_time to the next stop_time in the trip's stop_sequence.",
        blank=True,
        null=True,
        choices=(
            (0, "Continuous stopping pickup."),
            (1, "No continuous stopping pickup."),
            (2, "Must phone agency to arrange continuous stopping pickup."),
            (3, "Must coordinate with driver to arrange continuous stopping pickup."),
        ),
    )
    continuous_drop_off = gtfs_fields.GTFSEnumField(
        help_text="Indicates that the rider can alight from the transit vehicle at any point along the vehicle’s travel path as described by shapes.txt, from this stop_time to the next stop_time in the trip's stop_sequence.",
        blank=True,
        null=True,
        choices=(
            (0, "Continuous stopping drop off."),
            (1, "No continuous stopping drop off."),
            (2, "Must phone agency to arrange continuous stopping drop off."),
            (3, "Must coordinate with driver to arrange continuous stopping drop off."),
        ),
    )
    shape_dist_traveled = gtfs_fields.GTFSNonNegativeFloatField(
        help_text="Actual distance traveled along the associated shape, from the first stop to the stop specified in this record. This field specifies how much of the shape to draw between any two stops during a trip.",
        blank=True,
        null=True,
    )
    timepoint = gtfs_fields.GTFSEnumField(
        help_text="Indicates if arrival and departure times for a stop are strictly adhered to by the vehicle or if they are instead approximate and/or interpolated times.",
        blank=True,
        null=True,
        choices=(
            (0, "Times are considered approximate."),
            (1, "Times are considered exact."),
        ),
    )
    pickup_booking_rule_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the boarding booking rule at this stop time.",
        blank=True,
        null=True,
    )
    drop_off_booking_rule_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the alighting booking rule at this stop time.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseCalendarDate(models.Model):
    """Exceptions for the services defined in the calendar.txt.

    Maps to calendar_dates.txt.
    This model does not include primary keys."""

    service_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a set of dates when a service exception occurs for one or more routes. Each (service_id, date) pair may only appear once in calendar_dates.txt if using calendar.txt and calendar_dates.txt in conjunction. If a service_id value appears in both calendar.txt and calendar_dates.txt, the information in calendar_dates.txt modifies the service information specified in calendar.txt.",
    )
    date = gtfs_fields.GTFSDateField(
        help_text="Date when service exception occurs.",
    )
    exception_type = gtfs_fields.GTFSEnumField(
        help_text="Indicates whether service is available on the date specified in the date field.<hr>*Example:* Suppose a route has one set of trips available on holidays and another set of trips available on all other days. One service_id could correspond to the regular service schedule and another service_id could correspond to the holiday schedule. For a particular holiday, the calendar_dates.txt file could be used to add the holiday to the holiday service_id and to remove the holiday from the regular service_id schedule.",
        choices=(
            (1, "Service has been added for the specified date."),
            (2, "Service has been removed for the specified date."),
        ),
    )

    class Meta:
        abstract = True


class BaseFareAttribute(models.Model):
    """Fare information for a transit agency's routes.

    Maps to fare_attributes.txt.
    This model does not include primary keys."""

    fare_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a fare class.",
    )
    price = gtfs_fields.GTFSNonNegativeFloatField(
        help_text="Fare price, in the unit specified by currency_type.",
    )
    currency_type = gtfs_fields.GTFSCurrencyCodeField(
        help_text="Currency used to pay the fare.",
    )
    payment_method = gtfs_fields.GTFSEnumField(
        help_text="Indicates when the fare must be paid.",
        choices=(
            (0, "Fare is paid on board."),
            (1, "Fare must be paid before boarding."),
        ),
    )
    transfers = gtfs_fields.GTFSEnumField(
        help_text="Indicates the number of transfers permitted on this fare.",
        blank=True,
        null=True,
        choices=(
            (0, "No transfers permitted on this fare."),
            (1, "Riders may transfer once."),
            (2, "Riders may transfer twice."),
            (None, "Unlimited transfers are permitted."),
        ),
    )
    agency_id = gtfs_fields.GTFSIDField(
        help_text="Identifies the relevant agency for a fare.",
        blank=True,
        null=True,
    )
    transfer_duration = gtfs_fields.GTFSNonNegativeIntegerField(
        help_text="Length of time in seconds before a transfer expires. When transfers=0 this field may be used to indicate how long a ticket is valid for or it may be left empty.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseFareRule(models.Model):
    """Rules to apply fares for itineraries.

    Maps to fare_rules.txt.
    This model does not include primary keys."""

    fare_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a fare class.",
    )
    route_id = gtfs_fields.GTFSIDField(
        help_text='Identifies a route associated with the fare class. If several routes with the same fare attributes exist, create a record in fare_rules.txt for each route.<hr>*Example:* If fare class "b" is valid on route "TSW" and "TSE", fare_rules.txt would contain records: fare_id,route_id / b,TSW / b,TSE.',
        blank=True,
        null=True,
    )
    origin_id = gtfs_fields.GTFSIDField(
        help_text='Identifies an origin zone. If a fare class has multiple origin zones, create a record in fare_rules.txt for each origin_id.<hr>*Example:* If fare class "b" is valid for all travel originating from either zone "2" or zone "8", fare_rules.txt would contain: fare_id,...,origin_id / b,...,2 / b,...,8.',
        blank=True,
        null=True,
    )
    destination_id = gtfs_fields.GTFSIDField(
        help_text='Identifies a destination zone. If a fare class has multiple destination zones, create a record in fare_rules.txt for each destination_id.<hr>*Example:* The origin_id and destination_id fields can be used together to specify that fare class "b" is valid for travel between zones 3 and 4, and between zones 3 and 5: fare_id,...,origin_id,destination_id / b,...,3,4 / b,...,3,5.',
        blank=True,
        null=True,
    )
    contains_id = gtfs_fields.GTFSIDField(
        help_text='Identifies the zones that a rider will enter while using a given fare class. Used in some systems to calculate correct fare class.<hr>*Example:* If fare class "c" is associated with all travel on the GRT route that passes through zones 5, 6, and 7, fare_rules.txt would contain records for contains_id 5, 6, and 7. Because all contains_id zones must be matched for the fare to apply, an itinerary that passes through zones 5 and 6 but not zone 7 would not have fare class "c".',
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseTimeframe(models.Model):
    """Date and time periods to use in fare rules for fares that depend on date and time factors.

    Maps to timeframes.txt.
    This model does not include primary keys."""

    timeframe_group_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a timeframe or set of timeframes.",
    )
    start_time = gtfs_fields.GTFSLocalTimeField(
        help_text="Defines the beginning of a timeframe. The interval includes the start time. Values greater than 24:00:00 are forbidden. An empty value in start_time is considered 00:00:00.",
        blank=True,
        null=True,
    )
    end_time = gtfs_fields.GTFSLocalTimeField(
        help_text="Defines the end of a timeframe. The interval does not include the end time. Values greater than 24:00:00 are forbidden. An empty value in end_time is considered 24:00:00.",
        blank=True,
        null=True,
    )
    service_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a set of dates that a timeframe is in effect.",
    )

    class Meta:
        abstract = True


class BaseRiderCategory(models.Model):
    """Defines categories of riders (e.g. elderly, student).

    Maps to rider_categories.txt.
    This model does not include primary keys."""

    rider_category_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a rider category.",
    )
    rider_category_name = gtfs_fields.GTFSTextField(
        help_text="Rider category name as displayed to the rider.",
    )
    is_default_fare_category = gtfs_fields.GTFSEnumField(
        help_text="Specifies if an entry in rider_categories.txt should be considered the default category (i.e. the main category that should be displayed to riders). For example, Adult fare, Regular fare, etc. When multiple rider categories are eligible for a single fare product specified by a fare_product_id, there must be exactly one eligible rider category indicated as the default rider category (is_default_fare_category=1).",
        choices=(
            (0, "Category is not considered the default."),
            (1, "Category is considered the default one."),
        ),
    )
    eligibility_url = gtfs_fields.GTFSURLField(
        help_text="URL of a web page, usually from the operating agency, that provides detailed information about a specific rider category and/or describes its eligibility criteria.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseFareMedia(models.Model):
    """To describe the fare media that can be employed to use fare products.

    Maps to fare_media.txt.
    This model does not include primary keys."""

    fare_media_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a fare media.",
    )
    fare_media_name = gtfs_fields.GTFSTextField(
        help_text="Name of the fare media. For fare media which are transit cards (fare_media_type=2) or mobile apps (fare_media_type=4), fare_media_name should be included and should match the rider-facing name used by the organizations delivering them.",
        blank=True,
        null=True,
    )
    fare_media_type = gtfs_fields.GTFSEnumField(
        help_text="The type of fare media.",
        choices=(
            (
                0,
                "None. Used when there is no fare media involved in purchasing or validating a fare product, such as paying cash to a driver or conductor with no physical ticket provided.",
            ),
            (
                1,
                "Physical paper ticket that allows a passenger to take either a certain number of pre-purchased trips or unlimited trips within a fixed period of time.",
            ),
            (
                2,
                "Physical transit card that has stored tickets, passes or monetary value.",
            ),
            (
                3,
                "cEMV (contactless Europay, Mastercard and Visa) as an open-loop token container for account-based ticketing.",
            ),
            (
                4,
                "Mobile app that has stored virtual transit cards, tickets, passes, or monetary value.",
            ),
        ),
    )

    class Meta:
        abstract = True


class BaseFareProduct(models.Model):
    """To describe the different types of tickets or fares that can be purchased by riders.

    Maps to fare_products.txt.
    This model does not include primary keys."""

    fare_product_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a fare product or set of fare products. Multiple records sharing the same fare_product_id are permitted as long as they contain different fare_media_id values or rider_category_id values.",
    )
    fare_product_name = gtfs_fields.GTFSTextField(
        help_text="The name of the fare product as displayed to riders.",
        blank=True,
        null=True,
    )
    rider_category_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a rider category eligible for the fare product. If fare_products.rider_category_id is empty, the fare product is eligible for any rider_category_id. When multiple rider categories are eligible for a single fare product specified by a fare_product_id, there must be only one of these rider categories indicated as the default rider category (is_default_fare_category=1).",
        blank=True,
        null=True,
    )
    fare_media_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a fare media that can be employed to use the fare product during the trip. When fare_media_id is empty, it is considered that the fare media is unknown.",
        blank=True,
        null=True,
    )
    amount = gtfs_fields.GTFSCurrencyAmountField(
        help_text="The cost of the fare product. May be negative to represent transfer discounts. May be zero to represent a fare product that is free. The currency amount must contain the number of decimal places specified by the norm ISO 4217 for the accompanying Currency code.<hr>*Example:* If the fare is 2 US Dollars, the amount is 2.00 instead of 2.",
    )
    currency = gtfs_fields.GTFSCurrencyCodeField(
        help_text="The currency of the cost of the fare product.",
    )

    class Meta:
        abstract = True


class BaseFareLegRule(models.Model):
    """Fare rules for individual legs of travel.

    Maps to fare_leg_rules.txt.
    This model does not include primary keys."""

    leg_group_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a group of entries in fare_leg_rules.txt. Used to describe fare transfer rules between fare_transfer_rules.from_leg_group_id and fare_transfer_rules.to_leg_group_id. Multiple entries may belong to the same leg_group_id. The same entry (not including leg_group_id) must not belong to multiple leg_group_id values.",
        blank=True,
        null=True,
    )
    network_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a route network that applies for the fare leg rule. If rule_priority does not exist and there are no matching network_id values, empty network_id entries will be matched by default and correspond to all networks defined in routes.txt or networks.txt excluding the ones listed under fare_leg_rules.network_id. If rule_priority exists, an empty network_id indicates the route network of the leg does not affect the matching of this rule. When matching against an effective fare leg of multiple legs, each leg must have the same network_id.",
        blank=True,
        null=True,
    )
    from_area_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a departure area. If rule_priority does not exist and there are no matching from_area_id values, empty from_area_id entries will be matched by default and correspond to all areas defined in areas.area_id excluding the ones listed under fare_leg_rules.from_area_id. If rule_priority exists, an empty from_area_id indicates the departure area does not affect the matching of this rule. When matching against an effective fare leg of multiple legs, the first leg is used for determining the departure area.",
        blank=True,
        null=True,
    )
    to_area_id = gtfs_fields.GTFSIDField(
        help_text="Identifies an arrival area. If rule_priority does not exist and there are no matching to_area_id values, empty to_area_id entries will be matched by default and correspond to all areas defined in areas.area_id excluding the ones listed under fare_leg_rules.to_area_id. If rule_priority exists, an empty to_area_id indicates the arrival area does not affect the matching of this rule. When matching against an effective fare leg of multiple legs, the last leg is used for determining the arrival area.",
        blank=True,
        null=True,
    )
    from_timeframe_group_id = gtfs_fields.GTFSIDField(
        help_text="Defines the timeframe for the fare validation event at the start of the fare leg. The \"start time\" of the fare leg is the time at which the event is scheduled to occur. For example, the time could be the scheduled departure time of a bus at the start of a fare leg where the rider boards and validates their fare. The start time is computed in local time as determined by the Local Time Semantics of timeframes.txt, using the stop or station of the fare leg's departure event for timezone resolution when appropriate. For a fare leg rule that specifies a from_timeframe_group_id, the rule matches a leg if there exists a record in timeframes.txt where: (1) timeframe_group_id equals from_timeframe_group_id, (2) the set of days identified by service_id contains the current day of the fare leg's start time, and (3) the time-of-day of the fare leg's start time is greater than or equal to timeframes.start_time and less than timeframes.end_time. An empty from_timeframe_group_id indicates that the start time of the leg does not affect the matching of this rule. When matching against an effective fare leg of multiple legs, the first leg is used for determining the starting fare validation event.",
        blank=True,
        null=True,
    )
    to_timeframe_group_id = gtfs_fields.GTFSIDField(
        help_text="Defines the timeframe for the fare validation event at the end of the fare leg. The \"end time\" of the fare leg is the time at which the event is scheduled to occur. For example, the time could be the scheduled arrival time of a bus at the end of a fare leg where the rider gets off and validates their fare. The end time is computed in local time as determined by the Local Time Semantics of timeframes.txt, using the stop or station of the fare leg's arrival event for timezone resolution when appropriate. For a fare leg rule that specifies a to_timeframe_group_id, the rule matches a leg if there exists a record in timeframes.txt where: (1) timeframe_group_id equals to_timeframe_group_id, (2) the set of days identified by service_id contains the current day of the fare leg's end time, and (3) the time-of-day of the fare leg's end time is greater than or equal to timeframes.start_time and less than timeframes.end_time. An empty to_timeframe_group_id indicates that the end time of the leg does not affect the matching of this rule. When matching against an effective fare leg of multiple legs, the last leg is used for determining the ending fare validation event.",
        blank=True,
        null=True,
    )
    fare_product_id = gtfs_fields.GTFSIDField(
        help_text="The fare product required to travel the leg.",
    )
    rule_priority = gtfs_fields.GTFSNonNegativeIntegerField(
        help_text="Defines the order of priority in which matching rules are applied to legs, allowing certain rules to take precedence over others. When multiple entries match, the rule or set of rules with the highest value for rule_priority will be selected. An empty value for rule_priority is treated as zero.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseFareLegJoinRule(models.Model):
    """Rules for defining two or more legs should be considered as a single effective fare leg for the purposes of matching against rules in fare_leg_rules.txt

    Maps to fare_leg_join_rules.txt.
    This model does not include primary keys."""

    from_network_id = gtfs_fields.GTFSIDField(
        help_text="Matches a pre-transfer leg that uses the specified route network. If specified, the same to_network_id must also be specified.",
    )
    to_network_id = gtfs_fields.GTFSIDField(
        help_text="Matches a post-transfer leg that uses the specified route network. If specified, the same from_network_id must also be specified.",
    )
    from_stop_id = gtfs_fields.GTFSIDField(
        help_text="Matches a pre-transfer leg that ends at the specified stop (location_type=0 or empty) or station (location_type=1).",
        blank=True,
        null=True,
    )
    to_stop_id = gtfs_fields.GTFSIDField(
        help_text="Matches a post-transfer leg that starts at the specified stop (location_type=0 or empty) or station (location_type=1).",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseFareTransferRule(models.Model):
    """Fare rules for transfers between legs of travel.

    Maps to fare_transfer_rules.txt.
    This model does not include primary keys."""

    from_leg_group_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a group of pre-transfer fare leg rules. If there are no matching from_leg_group_id values to the leg_group_id being filtered, empty from_leg_group_id entries will be matched by default and correspond to all leg groups defined under fare_leg_rules.leg_group_id excluding the ones listed under fare_transfer_rules.from_leg_group_id.",
        blank=True,
        null=True,
    )
    to_leg_group_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a group of post-transfer fare leg rules. If there are no matching to_leg_group_id values to the leg_group_id being filtered, empty to_leg_group_id entries will be matched by default and correspond to all leg groups defined under fare_leg_rules.leg_group_id excluding the ones listed under fare_transfer_rules.to_leg_group_id.",
        blank=True,
        null=True,
    )
    transfer_count = gtfs_fields.GTFSNonZeroIntegerField(
        help_text="Defines how many consecutive transfers the transfer rule may be applied to. Valid options are -1 (no limit) or 1 or more (defines how many transfers the rule may span). If a sub-journey matches multiple records with different transfer_count values, the rule with the minimum transfer_count that is greater than or equal to the current transfer count of the sub-journey is selected.",
        blank=True,
        null=True,
    )
    duration_limit = gtfs_fields.GTFSPositiveIntegerField(
        help_text="Defines the duration limit of the transfer, expressed in integer increments of seconds. If there is no duration limit, duration_limit must be empty.",
        blank=True,
        null=True,
    )
    duration_limit_type = gtfs_fields.GTFSEnumField(
        help_text="Defines the relative start and end of duration_limit. When a transfer rule with the same from_leg_group_id and to_leg_group_id is matched multiple times consecutively within a multi-leg journey, the duration_limit specified by the rule should be measured starting from the first matched leg.",
        blank=True,
        null=True,
        choices=(
            (
                0,
                "Between the departure fare validation of the first leg in transfer sub-journey and the arrival fare validation of the last leg in transfer sub-journey.",
            ),
            (
                1,
                "Between the departure fare validation of the first leg in transfer sub-journey and the departure fare validation of the last leg in transfer sub-journey.",
            ),
            (
                2,
                "Between the arrival fare validation of the first leg in transfer sub-journey and the departure fare validation of the last leg in transfer sub-journey.",
            ),
            (
                3,
                "Between the arrival fare validation of the first leg in transfer sub-journey and the arrival fare validation of the last leg in transfer sub-journey.",
            ),
        ),
    )
    fare_transfer_type = gtfs_fields.GTFSEnumField(
        help_text="Indicates the cost processing method of transferring between legs in a journey.",
        choices=(
            (
                0,
                "From-leg fare_leg_rules.fare_product_id plus fare_transfer_rules.fare_product_id; A + AB.",
            ),
            (
                1,
                "From-leg fare_leg_rules.fare_product_id plus fare_transfer_rules.fare_product_id plus to-leg fare_leg_rules.fare_product_id; A + AB + B.",
            ),
            (2, "fare_transfer_rules.fare_product_id; AB."),
        ),
    )
    fare_product_id = gtfs_fields.GTFSIDField(
        help_text="The fare product required to transfer between two fare legs. If empty, the cost of the transfer rule is 0.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseArea(models.Model):
    """Area grouping of locations.

    Maps to areas.txt.
    This model does not include primary keys."""

    area_id = gtfs_fields.GTFSIDField(
        help_text="Identifies an area. Must be unique in areas.txt.",
    )
    area_name = gtfs_fields.GTFSTextField(
        help_text="The name of the area as displayed to the rider.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseStopArea(models.Model):
    """Rules to assign stops to areas.

    Maps to stop_areas.txt.
    This model does not include primary keys."""

    area_id = gtfs_fields.GTFSIDField(
        help_text="Identifies an area to which one or multiple stop_id values belong. The same stop_id may be defined in many area_id values.",
    )
    stop_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a stop. If a station (location_type=1) is defined in this field, all of its platforms are part of the same area unless platforms are assigned to other areas.",
    )

    class Meta:
        abstract = True


class BaseNetwork(models.Model):
    """Network grouping of routes.

    Maps to networks.txt.
    This model does not include primary keys."""

    network_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a network. Must be unique in networks.txt.",
    )
    network_name = gtfs_fields.GTFSTextField(
        help_text="The name of the network that applies for fare leg rules, as used by the local agency and its riders.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseRouteNetwork(models.Model):
    """Rules to assign routes to networks.

    Maps to route_networks.txt.
    This model does not include primary keys."""

    network_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a network to which one or multiple route_id values belong. A route_id can only be defined in one network_id.",
    )
    route_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a route.",
    )

    class Meta:
        abstract = True


class BaseFrequency(models.Model):
    """Headway (time between trips) for headway-based service or a compressed representation of fixed-schedule service.

    Maps to frequencies.txt.
    This model does not include primary keys."""

    trip_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a trip to which the specified headway of service applies.",
    )
    start_time = gtfs_fields.GTFSTimeField(
        help_text="Time at which the first vehicle departs from the first stop of the trip with the specified headway.",
    )
    end_time = gtfs_fields.GTFSTimeField(
        help_text="Time at which service changes to a different headway (or ceases) at the first stop in the trip.",
    )
    headway_secs = gtfs_fields.GTFSPositiveIntegerField(
        help_text="Time, in seconds, between departures from the same stop for the trip, during the time interval specified by start_time and end_time. Multiple headways may be defined for the same trip, but must not overlap. New headways may start at the exact time the previous headway ends.",
    )
    exact_times = gtfs_fields.GTFSEnumField(
        help_text="Indicates the type of service for a trip.",
        blank=True,
        null=True,
        choices=(
            (0, "Frequency-based trips."),
            (
                1,
                "Schedule-based trips with the exact same headway throughout the day. end_time must be greater than the last desired trip start_time but less than the last desired trip start_time + headway_secs.",
            ),
        ),
    )

    class Meta:
        abstract = True


class BaseTransfer(models.Model):
    """Rules for making connections at transfer points between routes.

    Maps to transfers.txt.
    This model does not include primary keys."""

    from_stop_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a stop (location_type=0) or a station (location_type=1) where a connection between routes begins. If this field refers to a station, the transfer rule applies to all its child stops. It must refer to a stop (location_type=0) if transfer_type is 4 or 5.",
        blank=True,
        null=True,
    )
    to_stop_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a stop (location_type=0) or a station (location_type=1) where a connection between routes ends. If this field refers to a station, the transfer rule applies to all its child stops. It must refer to a stop (location_type=0) if transfer_type is 4 or 5.",
        blank=True,
        null=True,
    )
    from_route_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a route where a connection begins. If from_route_id is defined, the transfer will apply to the arriving trip on the route for the given from_stop_id. If both from_trip_id and from_route_id are defined, the trip_id must belong to the route_id, and from_trip_id will take precedence.",
        blank=True,
        null=True,
    )
    to_route_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a route where a connection ends. If to_route_id is defined, the transfer will apply to the departing trip on the route for the given to_stop_id. If both to_trip_id and to_route_id are defined, the trip_id must belong to the route_id, and to_trip_id will take precedence.",
        blank=True,
        null=True,
    )
    from_trip_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a trip where a connection between routes begins. If from_trip_id is defined, the transfer will apply to the arriving trip for the given from_stop_id. If both from_trip_id and from_route_id are defined, the trip_id must belong to the route_id, and from_trip_id will take precedence.",
        blank=True,
        null=True,
    )
    to_trip_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a trip where a connection between routes ends. If to_trip_id is defined, the transfer will apply to the departing trip for the given to_stop_id. If both to_trip_id and to_route_id are defined, the trip_id must belong to the route_id, and to_trip_id will take precedence.",
        blank=True,
        null=True,
    )
    transfer_type = gtfs_fields.GTFSEnumField(
        help_text="Indicates the type of connection for the specified (from_stop_id, to_stop_id) pair.",
        choices=(
            (0, "Recommended transfer point between routes."),
            (
                1,
                "Timed transfer point between two routes. The departing vehicle is expected to wait for the arriving one and leave sufficient time for a rider to transfer between routes.",
            ),
            (
                2,
                "Transfer requires a minimum amount of time between arrival and departure to ensure a connection. The time required to transfer is specified by min_transfer_time.",
            ),
            (3, "Transfers are not possible between routes at the location."),
            (
                4,
                "Passengers can transfer from one trip to another by staying onboard the same vehicle (in-seat transfer).",
            ),
            (
                5,
                "In-seat transfers are not allowed between sequential trips. The passenger must alight from the vehicle and re-board.",
            ),
        ),
    )
    min_transfer_time = gtfs_fields.GTFSNonNegativeIntegerField(
        help_text="Amount of time, in seconds, that must be available to permit a transfer between routes at the specified stops. The min_transfer_time should be sufficient to permit a typical rider to move between the two stops, including buffer time to allow for schedule variance on each route.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BasePathway(models.Model):
    """Pathways linking together locations within stations.

    Maps to pathways.txt.
    This model does not include primary keys."""

    pathway_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a pathway. Must be unique in the dataset. Different pathways may have the same from_stop_id and to_stop_id values. Example: Two escalators side-by-side in opposite directions, or a stair set and elevator going from the same place to the same place.",
    )
    from_stop_id = gtfs_fields.GTFSIDField(
        help_text="Location at which the pathway begins. Must identify a platform (location_type=0 or empty), entrance/exit (location_type=2), generic node (location_type=3) or boarding area (location_type=4). Values that identify stations (location_type=1), or stops with stop_access=1, are forbidden.",
    )
    to_stop_id = gtfs_fields.GTFSIDField(
        help_text="Location at which the pathway ends. Must identify a platform (location_type=0 or empty), entrance/exit (location_type=2), generic node (location_type=3) or boarding area (location_type=4). Values that identify stations (location_type=1), or stops with stop_access=1, are forbidden.",
    )
    pathway_mode = gtfs_fields.GTFSEnumField(
        help_text="Type of pathway between the specified (from_stop_id, to_stop_id) pair.",
        choices=(
            (1, "Walkway."),
            (2, "Stairs."),
            (3, "Moving sidewalk/travelator."),
            (4, "Escalator."),
            (5, "Elevator."),
            (
                6,
                "Fare gate (or payment gate): A pathway that crosses into an area of the station where proof of payment is required. Fare gates may separate paid areas of the station from unpaid ones, or separate different payment areas within the same station from each other.",
            ),
            (
                7,
                "Exit gate: A pathway exiting a paid area into an unpaid area where proof of payment is not required to cross.",
            ),
        ),
    )
    is_bidirectional = gtfs_fields.GTFSEnumField(
        help_text="Indicates the direction that the pathway can be taken.",
        choices=(
            (
                0,
                "Unidirectional pathway that can only be used from from_stop_id to to_stop_id.",
            ),
            (
                1,
                "Bidirectional pathway that can be used in both directions. Exit gates (pathway_mode=7) must not be bidirectional.",
            ),
        ),
    )
    length = gtfs_fields.GTFSNonNegativeFloatField(
        help_text="Horizontal length in meters of the pathway from the origin location to the destination location. Recommended for walkways (pathway_mode=1), fare gates (pathway_mode=6) and exit gates (pathway_mode=7).",
        blank=True,
        null=True,
    )
    traversal_time = gtfs_fields.GTFSPositiveIntegerField(
        help_text="Average time in seconds needed to walk through the pathway from the origin location to the destination location. Recommended for moving sidewalks (pathway_mode=3), escalators (pathway_mode=4) and elevators (pathway_mode=5).",
        blank=True,
        null=True,
    )
    stair_count = gtfs_fields.GTFSIntegerField(
        help_text="Number of stairs of the pathway. A positive value implies walking up; a negative value implies walking down. Recommended for stairs (pathway_mode=2). If only an estimated stair count can be provided, approximate 15 stairs for 1 floor.",
        blank=True,
        null=True,
    )
    max_slope = gtfs_fields.GTFSFloatField(
        help_text="Maximum slope ratio of the pathway. Valid options are 0 or empty for no slope, or a float ratio (positive for upwards, negative for downwards). Should only be used with walkways (pathway_mode=1) and moving sidewalks (pathway_mode=3). Example: 0.083 (8.3%) is the maximum slope ratio for hand-propelled wheelchairs in the US.",
        blank=True,
        null=True,
    )
    min_width = gtfs_fields.GTFSPositiveFloatField(
        help_text="Minimum width of the pathway in meters. Recommended if the minimum width is less than 1 meter.",
        blank=True,
        null=True,
    )
    signposted_as = gtfs_fields.GTFSTextField(
        help_text='Public facing text from physical signage that is visible to riders. The text should appear exactly how it is printed on the signs. May be used to provide text directions to riders, such as "follow signs to". When the physical signage is multilingual, this field may be populated and translated following the example of stops.stop_name in feed_info.feed_lang.',
        blank=True,
        null=True,
    )
    reversed_signposted_as = gtfs_fields.GTFSTextField(
        help_text="Same as signposted_as, but when the pathway is used from the to_stop_id to the from_stop_id.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseLevel(models.Model):
    """Levels within stations.

    Maps to levels.txt.
    This model does not include primary keys."""

    level_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a level in a station.",
    )
    level_index = gtfs_fields.GTFSFloatField(
        help_text="Numeric index of the level that indicates its relative position. Ground level should have index 0, with levels above ground indicated by positive indices and levels below ground by negative indices.",
    )
    level_name = gtfs_fields.GTFSTextField(
        help_text="Name of the level as seen by the rider inside the building or station.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseLocationGroup(models.Model):
    """A group of stops that together indicate locations where a rider may request pickup or drop off.

    Maps to location_groups.txt.
    This model does not include primary keys."""

    location_group_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a location group. ID must be unique across all stops.stop_id, locations.geojson id, and location_groups.location_group_id values.",
    )
    location_group_name = gtfs_fields.GTFSTextField(
        help_text="The name of the location group as displayed to the rider.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseLocationGroupStop(models.Model):
    """Rules to assign stops to location groups.

    Maps to location_group_stops.txt.
    This model does not include primary keys."""

    location_group_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a location group to which one or multiple stop_id values belong. The same stop_id may be defined in many location_group_id values.",
    )
    stop_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a stop belonging to the location group.",
    )

    class Meta:
        abstract = True


class BaseLocation(models.Model):
    """Zones for rider pickup or drop-off requests by on-demand services, represented as GeoJSON polygons.

    Maps to locations.geojson.
    This model does not include primary keys."""

    type = gtfs_fields.GTFSTextField(
        help_text='"FeatureCollection" of locations.',
    )
    features = models.JSONField(
        help_text='Collection of "Feature" objects describing the locations.',
    )

    class Meta:
        abstract = True


class BaseBookingRule(models.Model):
    """Booking information for rider-requested services.

    Maps to booking_rules.txt.
    This model does not include primary keys."""

    booking_rule_id = gtfs_fields.GTFSIDField(
        help_text="Identifies a rule.",
    )
    booking_type = gtfs_fields.GTFSEnumField(
        help_text="Indicates how far in advance booking can be made.",
        choices=(
            (0, "Real time booking."),
            (1, "Up to same-day booking with advance notice."),
            (2, "Up to prior day(s) booking."),
        ),
    )
    prior_notice_duration_min = gtfs_fields.GTFSIntegerField(
        help_text="Minimum number of minutes before travel to make the request.",
        blank=True,
        null=True,
    )
    prior_notice_duration_max = gtfs_fields.GTFSIntegerField(
        help_text="Maximum number of minutes before travel to make the booking request.",
        blank=True,
        null=True,
    )
    prior_notice_last_day = gtfs_fields.GTFSIntegerField(
        help_text="Last day before travel to make the booking request. Example: Ride must be booked 1 day in advance before 5PM will be encoded as prior_notice_last_day=1.",
        blank=True,
        null=True,
    )
    prior_notice_last_time = gtfs_fields.GTFSTimeField(
        help_text="Last time on the last day before travel to make the booking request. Example: Ride must be booked 1 day in advance before 5PM will be encoded as prior_notice_last_time=17:00:00.",
        blank=True,
        null=True,
    )
    prior_notice_start_day = gtfs_fields.GTFSIntegerField(
        help_text="Earliest day before travel to make the booking request. Example: Ride can be booked at the earliest one week in advance at midnight will be encoded as prior_notice_start_day=7.",
        blank=True,
        null=True,
    )
    prior_notice_start_time = gtfs_fields.GTFSTimeField(
        help_text="Earliest time on the earliest day before travel to make the booking request. Example: Ride can be booked at the earliest one week in advance at midnight will be encoded as prior_notice_start_time=00:00:00.",
        blank=True,
        null=True,
    )
    prior_notice_service_id = gtfs_fields.GTFSIDField(
        help_text="Indicates the service days on which prior_notice_last_day or prior_notice_start_day are counted. Example: If empty, prior_notice_start_day=2 will be two calendar days in advance. If defined as a service_id containing only business days, prior_notice_start_day=2 will be two business days in advance.",
        blank=True,
        null=True,
    )
    message = gtfs_fields.GTFSTextField(
        help_text="Message to riders utilizing service at a stop_time when booking on-demand pickup and drop off.",
        blank=True,
        null=True,
    )
    pickup_message = gtfs_fields.GTFSTextField(
        help_text="Functions in the same way as message but used when riders have on-demand pickup only.",
        blank=True,
        null=True,
    )
    drop_off_message = gtfs_fields.GTFSTextField(
        help_text="Functions in the same way as message but used when riders have on-demand drop off only.",
        blank=True,
        null=True,
    )
    phone_number = gtfs_fields.GTFSPhoneNumberField(
        help_text="Phone number to call to make the booking request.",
        blank=True,
        null=True,
    )
    info_url = gtfs_fields.GTFSURLField(
        help_text="URL providing information about the booking rule.",
        blank=True,
        null=True,
    )
    booking_url = gtfs_fields.GTFSURLField(
        help_text="URL to an online interface or app where the booking request can be made.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseTranslation(models.Model):
    """Translations of customer-facing dataset values.

    Maps to translations.txt.
    This model does not include primary keys."""

    table_name = gtfs_fields.GTFSTextField(
        help_text="Defines the table that contains the field to be translated. Any file added to GTFS will have a table_name value equivalent to the file name, not including the .txt file extension.",
        choices=(
            ("agency", "agency.txt."),
            ("stops", "stops.txt."),
            ("routes", "routes.txt."),
            ("trips", "trips.txt."),
            ("stop_times", "stop_times.txt."),
            ("pathways", "pathways.txt."),
            ("levels", "levels.txt."),
            ("feed_info", "feed_info.txt."),
            ("attributions", "attributions.txt."),
        ),
    )
    field_name = gtfs_fields.GTFSTextField(
        help_text="Name of the field to be translated. Fields with type Text may be translated; URL, Email, and Phone number may be translated to provide resources in the correct language. Other types should not be translated.",
    )
    language = gtfs_fields.GTFSLanguageCodeField(
        help_text="Language of translation. If the language is the same as in feed_info.feed_lang, the original value of the field is assumed to be the default value to use in languages without specific translations (if default_lang does not specify otherwise).",
    )
    translation = gtfs_fields.GTFSTextField(
        help_text="Translated value.",
    )
    record_id = gtfs_fields.GTFSIDField(
        help_text="Defines the record that corresponds to the field to be translated. The value must be the first or only field of a table's primary key as defined in the reference.",
        blank=True,
        null=True,
    )
    record_sub_id = gtfs_fields.GTFSIDField(
        help_text="Secondary ID of the table when the table does not have a unique ID, as defined in the reference.",
        blank=True,
        null=True,
    )
    field_value = gtfs_fields.GTFSTextField(
        help_text="Defines the value which should be translated instead of identifying a specific record. The translation is applied when the field identified by table_name and field_name contains the exact same value defined in field_value. If only a subset of the value matches field_value, the translation is not applied. If two translation rules match the same record (one with field_value and one with record_id), the rule with record_id takes precedence.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseFeedInfo(models.Model):
    """Dataset metadata, including publisher, version, and expiration information.

    Maps to feed_info.txt.
    This model does not include primary keys."""

    feed_publisher_name = gtfs_fields.GTFSTextField(
        help_text="Full name of the organization that publishes the dataset.",
    )
    feed_publisher_url = gtfs_fields.GTFSURLField(
        help_text="URL of the dataset publishing organization's website.",
    )
    feed_lang = gtfs_fields.GTFSLanguageCodeField(
        help_text="Default language used for the text in this dataset. The translations.txt file can be used if the text needs to be translated into languages other than the default one. The default language may be multilingual for datasets with the original text in multiple languages; in such cases, feed_lang should contain mul and translations should be provided for each language used in the dataset. If all original text is in the same language, then mul should not be used.",
    )
    default_lang = gtfs_fields.GTFSLanguageCodeField(
        help_text="Defines the language that should be used when the data consumer does not know the language of the rider.",
        blank=True,
        null=True,
    )
    feed_start_date = gtfs_fields.GTFSDateField(
        help_text="The dataset provides complete and reliable schedule information for service in the period from the beginning of the feed_start_date day to the end of the feed_end_date day. Both days may be left empty if unavailable. The feed_end_date date must not precede the feed_start_date date if both are given. It is recommended that dataset providers give schedule data outside this period to advise of likely future service, but dataset consumers should treat it mindful of its non-authoritative status. If feed_start_date or feed_end_date extend beyond the active calendar dates defined in calendar.txt and calendar_dates.txt, the dataset asserts there is no service for dates within the feed range but not included in the active calendar dates.",
        blank=True,
        null=True,
    )
    feed_end_date = gtfs_fields.GTFSDateField(
        help_text="End date for the feed period described by feed_start_date.",
        blank=True,
        null=True,
    )
    feed_version = gtfs_fields.GTFSTextField(
        help_text="String that indicates the current version of the GTFS dataset.",
        blank=True,
        null=True,
    )
    feed_contact_email = gtfs_fields.GTFSEmailField(
        help_text="Email address for communication regarding the GTFS dataset and data publishing practices. feed_contact_email is a technical contact for GTFS-consuming applications. Provide customer service contact information through agency.txt. It is recommended that at least one of feed_contact_email or feed_contact_url are provided.",
        blank=True,
        null=True,
    )
    feed_contact_url = gtfs_fields.GTFSURLField(
        help_text="URL for contact information regarding the GTFS dataset and data publishing practices. feed_contact_url is a technical contact for GTFS-consuming applications. Provide customer service contact information through agency.txt. It is recommended that at least one of feed_contact_url or feed_contact_email are provided.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True


class BaseAttribution(models.Model):
    """Dataset attributions.

    Maps to attributions.txt.
    This model does not include primary keys."""

    attribution_id = gtfs_fields.GTFSIDField(
        help_text="Identifies an attribution for the dataset or a subset of it.",
        blank=True,
        null=True,
    )
    agency_id = gtfs_fields.GTFSIDField(
        help_text="Agency to which the attribution applies. If one agency_id, route_id, or trip_id attribution is defined, the other ones must be empty. If none is specified, the attribution applies to the whole dataset.",
        blank=True,
        null=True,
    )
    route_id = gtfs_fields.GTFSIDField(
        help_text="Functions in the same way as agency_id except the attribution applies to a route.",
        blank=True,
        null=True,
    )
    trip_id = gtfs_fields.GTFSIDField(
        help_text="Functions in the same way as agency_id except the attribution applies to a trip.",
        blank=True,
        null=True,
    )
    organization_name = gtfs_fields.GTFSTextField(
        help_text="Name of the organization that the dataset is attributed to.",
    )
    is_producer = gtfs_fields.GTFSEnumField(
        help_text="The role of the organization is producer. At least one of is_producer, is_operator, or is_authority should be set to 1.",
        blank=True,
        null=True,
        choices=(
            (0, "Organization does not have this role."),
            (1, "Organization does have this role."),
        ),
    )
    is_operator = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as is_producer except the role of the organization is operator.",
        blank=True,
        null=True,
        choices=(
            (0, "Organization does not have this role."),
            (1, "Organization does have this role."),
        ),
    )
    is_authority = gtfs_fields.GTFSEnumField(
        help_text="Functions in the same way as is_producer except the role of the organization is authority.",
        blank=True,
        null=True,
        choices=(
            (0, "Organization does not have this role."),
            (1, "Organization does have this role."),
        ),
    )
    attribution_url = gtfs_fields.GTFSURLField(
        help_text="URL of the organization.",
        blank=True,
        null=True,
    )
    attribution_email = gtfs_fields.GTFSEmailField(
        help_text="Email of the organization.",
        blank=True,
        null=True,
    )
    attribution_phone = gtfs_fields.GTFSPhoneNumberField(
        help_text="Phone number of the organization.",
        blank=True,
        null=True,
    )

    class Meta:
        abstract = True
