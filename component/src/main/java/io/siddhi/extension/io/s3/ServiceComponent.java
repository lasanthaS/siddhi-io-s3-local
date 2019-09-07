package io.siddhi.extension.io.s3;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;

@Component(immediate = true)
public class ServiceComponent {

    @Activate
    protected void activate(BundleContext bundleContext) {
        System.out.println(">>>>>>>>>>> Service component activated.");
    }

    @Deactivate
    protected void deactivated(BundleContext bundleContext) {
        System.out.println(">>>>>>>>>>> Service component deactivated.");
    }
}
