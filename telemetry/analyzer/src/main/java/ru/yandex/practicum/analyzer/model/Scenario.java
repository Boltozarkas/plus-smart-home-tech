package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "scenarios")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Scenario {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;

    @Column(nullable = false)
    private String name;

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @Builder.Default
    private List<ScenarioCondition> conditions = new ArrayList<>();

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @Builder.Default
    private List<ScenarioAction> actions = new ArrayList<>();
}