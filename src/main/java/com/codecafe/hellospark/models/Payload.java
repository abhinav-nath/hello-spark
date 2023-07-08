package com.codecafe.hellospark.models;

import lombok.Data;

@Data
public class Payload {
    private String orderId;
    private String country;
    private String email;
    private String cardNumber;
    private String cardIssuerCountry;
    private String paymentChannel;
}