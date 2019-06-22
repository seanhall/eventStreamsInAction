package nile.events;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.codehaus.jackson.type.TypeReference;
import nile.events.Event;

public class AbandonedCartEvent extends Event { // inherits from parent class Event
  public final DirectObject directObject; // static is like const, can't reinit; final is like double const, can't change value

  public AbandonedCartEvent(String shopper, String cart) { // main() will invoke with shopper and cart args
    super(shopper, "abandon"); // will invoke an instance of the parent Event class: public Event(String shopper, String verb) {
    this.directObject = new DirectObject(cart); // will pass cart value to DirectObject class below
  }

  public static final class DirectObject { // static class can be accessed without instantiating outer class
    public final Cart cart;

    public DirectObject(String cart) { // constructor method, will receive String cart from AbandonedCartEvent constructor above
      this.cart = new Cart(cart);
    }

    public static final class Cart {
      private static final int ABANDONED_AFTER_SECS = 1800;

      public List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();

      public Cart(String json) {
        if (json != null) {
          try {
            this.items = MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
          } catch (IOException ioe) {
            throw new RuntimeException("Problem parsing JSON cart", ioe);
          }
        }
      }

      public void addItem(Map<String, Object> item) {
        this.items.add(item);
      }

      public String asJson() { // stringifies cart json
        try {
          return MAPPER.writeValueAsString(this.items);
        } catch (IOException ioe) {
          throw new RuntimeException("Problem writing JSON cart", ioe);
        }
      }

      public static boolean isAbandoned(String timestamp) {
        DateTime ts = EVENT_DTF.parseDateTime(timestamp); // DateTimeFormatter EVENT_DTF
        DateTime cutoff = new DateTime(DateTimeZone.UTC).minusSeconds(ABANDONED_AFTER_SECS); // subtract 30m from now
        return ts.isBefore(cutoff); // if true, and timestamp is > 30m ago, isAbandoned
      }
    }
  }
}