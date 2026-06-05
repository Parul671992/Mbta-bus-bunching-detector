package com.parul.bunching.alerts;

import com.parul.bunching.common.events.BunchingAlert;
import com.parul.bunching.common.events.VehiclePosition;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class AlertsController {

    private final AlertStore store;

    public AlertsController(AlertStore store) {
        this.store = store;
    }

    /** Most recent alerts first, optionally for one route. Example: /api/alerts?route=39 */
    @GetMapping("/alerts")
    public List<BunchingAlert> alerts(@RequestParam(required = false) String route,
                                      @RequestParam(defaultValue = "50") int limit) {
        return store.alerts(route, Math.min(limit, 500));
    }

    /** Alert counts per route, worst route first. */
    @GetMapping("/alerts/summary")
    public Map<String, Integer> summary() {
        return store.alertCountsByRoute();
    }

    /** Latest known position of each active bus. */
    @GetMapping("/vehicles")
    public List<VehiclePosition> vehicles(@RequestParam(required = false) String route) {
        return store.liveVehicles(route);
    }
}
